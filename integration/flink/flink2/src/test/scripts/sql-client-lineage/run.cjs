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
function run(dir, name, sql) {
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
  assert.equal(result.status, 0, log);
  assert.ok(!output.includes('[ERROR]'), log);
  return output;
}
const direct = prepare('direct');
run(direct.dir, 'execute', direct.sql);
const directFields = verify(direct.dir);
console.log('PASS: SQL Client direct batch execution, real data and complete lineage');

function removeLineage(node, key) {
  if (!node || typeof node !== 'object') return 0;
  let count = Object.hasOwn(node, key) ? 1 : 0;
  delete node[key];
  for (const child of Object.values(node)) count += removeLineage(child, key);
  return count;
}
for (const mode of ['incomplete', 'legacy']) {
  const negative = prepare(mode);
  const gatePlan = path.join(negative.dir, 'gate-plan.json');
  run(negative.dir, 'compile-gate', negative.sql.split('EXECUTE STATEMENT SET')[0]
    + "COMPILE PLAN '" + gatePlan + "' FOR INSERT INTO Detail SELECT order_id, 'fixed', amount + fee FROM Orders;\n");
  const corrupted = JSON.parse(fs.readFileSync(gatePlan, 'utf8'));
  assert.equal(removeLineage(corrupted, 'columnLineage'), 1);
  if (mode === 'legacy') assert.equal(removeLineage(corrupted, 'tableLineage'), 1);
  const badPlan = path.join(negative.dir, 'incomplete-plan.json');
  fs.writeFileSync(badPlan, JSON.stringify(corrupted));
  run(negative.dir, mode, negative.sql.split('CREATE TABLE Orders')[0] + "EXECUTE PLAN '" + badPlan + "';\n");
  verify.incomplete(negative.dir, mode === 'legacy');
  console.log('PASS: ' + mode + ' plan executes with unavailable columns and honest independent table status');
}

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
const mixedTemplate = fs.readFileSync(path.join(__dirname, 'mixed.sql'), 'utf8');
let mixedFields;
for (const mode of ['mixed', 'mixed-restored', 'partial-table']) {
  const dir = path.join(root, mode);
  fs.mkdirSync(dir);
  fs.writeFileSync(path.join(dir, 'numbers.csv'), '1\n2\n3\n');
  fs.writeFileSync(path.join(dir, 'other-numbers.csv'), '2\n3\n4\n');
  let sql = mixedTemplate.replaceAll('__ROOT__', dir.replaceAll("'", "''"));
  if (mode === 'partial-table') sql = sql.replace(
    'INSERT INTO Unsupported SELECT n.`value` FROM Numbers n WHERE EXISTS (SELECT 1 FROM OtherNumbers r WHERE r.`value` = n.`value`);',
    'INSERT INTO Unsupported SELECT `value` + 1 FROM OtherNumbers;');
  if (mode === 'mixed') {
    run(dir, 'execute', sql);
    mixedFields = verify.mixed(dir);
  } else {
    const mixedPlan = path.join(dir, 'plan.json');
    run(dir, 'compile', sql.replace('EXECUTE STATEMENT SET', "COMPILE PLAN '" + mixedPlan + "' FOR STATEMENT SET"));
    assert.ok(!fs.existsSync(path.join(dir, 'events.jsonl')), 'Mixed compilation must not submit');
    if (mode === 'partial-table') {
      const plan = JSON.parse(fs.readFileSync(mixedPlan, 'utf8'));
      const sink = plan.nodes.map(node => node.dynamicTableSink).find(sink =>
        sink?.tableLineage?.sinkKey === '`default_catalog`.`lineage_acceptance`.`Unsupported`');
      assert.ok(sink?.columnLineage && sink?.tableLineage, 'Both independent sink metadata blocks exist before removal');
      delete sink.columnLineage;
      delete sink.tableLineage;
      fs.writeFileSync(path.join(dir, 'original-plan.json'), fs.readFileSync(mixedPlan));
      fs.writeFileSync(mixedPlan, JSON.stringify(plan));
    }
    run(dir, 'restore', sql.split('CREATE DATABASE')[0] + "EXECUTE PLAN '" + mixedPlan + "';\n");
    if (mode === 'partial-table') verify.mixed(dir, true);
    else assert.deepEqual(verify.mixed(dir), mixedFields, 'Mixed sink lineage survives compiled restore');
  }
  console.log('PASS: ' + mode + ' exact data and per-sink table/column availability');
}
console.log('ALL SQL CLIENT ACCEPTANCE CHECKS PASSED: ' + root);
