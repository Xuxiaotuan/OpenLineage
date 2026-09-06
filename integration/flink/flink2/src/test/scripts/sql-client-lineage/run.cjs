// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const {spawnSync} = require('node:child_process');
const verify = require('./verify.cjs');

const [distribution, adapter] = process.argv.slice(2);
assert.ok(distribution && adapter, 'Usage: node run.cjs <paired Flink distribution> <OpenLineage Jar>');
assert.ok(fs.existsSync(path.join(distribution, 'bin/sql-client.sh')));
assert.ok(fs.statSync(adapter).isFile());
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ol-cli-'));
console.log('Evidence directory: ' + root);
const flink = path.join(root, 'flink');
fs.cpSync(distribution, flink, {recursive: true});
fs.copyFileSync(adapter, path.join(flink, 'lib', path.basename(adapter)));
const template = fs.readFileSync(path.join(__dirname, 'session.sql'), 'utf8');
function prepare(mode) {
  const dir = path.join(root, mode);
  fs.mkdirSync(dir);
  for (const file of ['orders.csv', 'customers.csv']) {
    fs.copyFileSync(path.join(__dirname, file), path.join(dir, file));
  }
  return {dir, sql: template.replaceAll('__ROOT__', dir.replaceAll("'", "''"))};
}
function run(dir, name, sql, expectFailure = false) {
  const file = path.join(dir, name + '.sql');
  fs.writeFileSync(file, sql);
  const log = path.join(dir, name + '.log');
  const fd = fs.openSync(log, 'w');
  let result;
  try {
    result = spawnSync('bash', [path.join(flink, 'bin/sql-client.sh'), '-hist', path.join(dir, 'history'), '-f', file], {
      cwd: dir, env: process.env, stdio: ['ignore', fd, fd], timeout: 120000
    });
  } finally {
    fs.closeSync(fd);
  }
  assert.ifError(result.error);
  const output = fs.readFileSync(log, 'utf8');
  if (!expectFailure) {
    assert.equal(result.status, 0, log);
    assert.ok(!output.includes('[ERROR]'), log);
  }
  return output;
}
const direct = prepare('direct');
run(direct.dir, 'execute', direct.sql);
const directFields = verify(direct.dir);
console.log('PASS: SQL Client direct batch execution, real data and complete lineage');

const negative = prepare('incomplete');
const gatePlan = path.join(negative.dir, 'gate-plan.json');
run(negative.dir, 'compile-gate', negative.sql.split('EXECUTE STATEMENT SET')[0]
  + "COMPILE PLAN '" + gatePlan + "' FOR INSERT INTO Detail SELECT order_id, 'fixed', amount + fee FROM Orders;\n");
const corrupted = JSON.parse(fs.readFileSync(gatePlan, 'utf8'));
function removeLineage(node) {
  if (!node || typeof node !== 'object') return 0;
  let count = Object.hasOwn(node, 'columnLineage') ? 1 : 0;
  delete node.columnLineage;
  for (const child of Object.values(node)) count += removeLineage(child);
  return count;
}
assert.equal(removeLineage(corrupted), 1);
const badPlan = path.join(negative.dir, 'incomplete-plan.json');
fs.writeFileSync(badPlan, JSON.stringify(corrupted));
const failedOutput = run(negative.dir, 'reject', negative.sql.split('CREATE TABLE Orders')[0] + "EXECUTE PLAN '" + badPlan + "';\n", true);
assert.match(failedOutput, /compiled plan does not contain complete column lineage/);
assert.ok(failedOutput.includes('[ERROR]'), 'SQL Client must report rejection');
assert.ok(!failedOutput.includes('Complete execution of the SQL update statement'));
assert.ok(!fs.existsSync(path.join(negative.dir, 'events.jsonl')));
function hasFiles(dir) {
  return fs.existsSync(dir) && fs.readdirSync(dir, {withFileTypes: true}).some(entry =>
    !entry.isDirectory() || hasFiles(path.join(dir, entry.name)));
}
// The filesystem connector can create empty staging directories while compiling.
for (const sink of ['detail', 'summary']) {
  assert.ok(!hasFiles(path.join(negative.dir, sink)), 'Rejected plan must not write Sink files');
}
console.log('PASS: incomplete lineage rejected, no event or Sink output');

const restored = prepare('restored');
const planFile = path.join(restored.dir, 'plan.json');
run(restored.dir, 'compile', restored.sql.replace('EXECUTE STATEMENT SET', "COMPILE PLAN '" + planFile + "' FOR STATEMENT SET") + '\nDROP TEMPORARY VIEW Enriched;\n');
assert.ok(!fs.existsSync(path.join(restored.dir, 'events.jsonl')), 'Compilation must not submit a job');
const plan = JSON.parse(fs.readFileSync(planFile, 'utf8'));
// These small CSV inputs naturally choose NestedLoopJoin, not AdaptiveJoin.
// Keep the default optimizer settings and require the fused node under test.
assert.ok(plan.nodes.some(node => node.type === 'batch-exec-multiple-input_1'),
  'The restore fixture must exercise MultipleInput subgraph serialization');
const prelude = restored.sql.split('CREATE TABLE Orders')[0];
run(restored.dir, 'restore', prelude + "EXECUTE PLAN '" + planFile + "';\n");
assert.deepEqual(verify(restored.dir), directFields, 'Direct and restored field lineage must match');
console.log('PASS: disk plan restored in a fresh SQL Client process without original views/tables');
console.log('ALL SQL CLIENT ACCEPTANCE CHECKS PASSED: ' + root);
