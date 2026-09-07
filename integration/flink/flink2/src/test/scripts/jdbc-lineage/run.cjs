// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const {execFileSync, spawnSync} = require('node:child_process');

const [distribution, adapter, jarDirectory, container] = process.argv.slice(2);
assert.ok(distribution && adapter && jarDirectory && container,
  'Usage: node run.cjs <Flink distribution> <adapter Jar> <JDBC Jars directory> <dedicated PostgreSQL container>');
assert.match(container, /^lineage-jdbc-poc-/);
const docker = args => execFileSync('docker', args, {encoding: 'utf8'}).trim();
const endpoint = docker(['port', container, '5432']);
assert.match(endpoint, /^127\.0\.0\.1:\d+$/);
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ol-jdbc-'));
console.log('Evidence directory: ' + root);
const schema = 'lineage_' + Date.now();
const namespace = 'postgres://' + endpoint;
const identity = table => 'lineage_poc.' + schema + '.' + table;
const psql = sql => docker(['exec', container, 'psql', '-U', 'postgres', '-d', 'lineage_poc',
  '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', sql]);
psql(`CREATE SCHEMA ${schema};
CREATE TABLE ${schema}.orders (order_id BIGINT, customer_id BIGINT, amount BIGINT, fee BIGINT);
CREATE TABLE ${schema}.customers (customer_id BIGINT, tier TEXT);
CREATE TABLE ${schema}.detail (order_id BIGINT, tier TEXT, net_amount BIGINT);
CREATE TABLE ${schema}.summary (tier TEXT, total_amount BIGINT, order_count BIGINT);
CREATE TABLE ${schema}.set_left (v BIGINT, tag TEXT);
CREATE TABLE ${schema}.set_right (v BIGINT, tag TEXT);
CREATE TABLE ${schema}.intersection (v BIGINT, tag TEXT);
CREATE TABLE ${schema}.intersection_all (v BIGINT, tag TEXT);
CREATE TABLE ${schema}.difference (v BIGINT, tag TEXT);
CREATE TABLE ${schema}.difference_all (v BIGINT, tag TEXT);
INSERT INTO ${schema}.orders VALUES (1,10,100,5),(2,20,200,5),(3,10,50,5);
INSERT INTO ${schema}.customers VALUES (10,'gold'),(20,'blocked');
INSERT INTO ${schema}.set_left VALUES (1,'a'),(1,'a'),(1,'a'),(2,'b'),(NULL,'z'),(3,'c');
INSERT INTO ${schema}.set_right VALUES (1,'a'),(1,'a'),(NULL,'z'),(2,'x');`);
const flink = path.join(root, 'flink');
fs.cpSync(distribution, flink, {recursive: true});
for (const jar of [adapter, ...fs.readdirSync(jarDirectory).filter(f => f.endsWith('.jar')).map(f => path.join(jarDirectory, f))]) {
  fs.copyFileSync(jar, path.join(flink, 'lib', path.basename(jar)));
}
const prelude = dir => `SET 'execution.target' = 'local';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'rest.bind-address' = '127.0.0.1';
SET 'rest.bind-port' = '0';
SET 'table.dml-sync' = 'true';
SET 'execution.job-status-changed-listeners' = 'io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory';
SET 'openlineage.transport.type' = 'file';
SET 'openlineage.transport.location' = '${dir}/events.jsonl';
SET 'openlineage.flink.disableCheckpointTracking' = 'true';
`;
const table = (logical, physical, columns) => `CREATE TABLE ${logical} (${columns}) WITH (
'connector'='jdbc', 'url'='jdbc:postgresql://${endpoint}/lineage_poc',
'table-name'='${schema}.${physical}', 'username'='postgres', 'password'='poc-only',
'sink.buffer-flush.max-rows'='1');\n`;
const ddl = table('LogicalOrders', 'orders', 'order_id BIGINT, customer_id BIGINT, amount BIGINT, fee BIGINT')
  + table('LogicalCustomers', 'customers', 'customer_id BIGINT, tier STRING')
  + table('LogicalDetail', 'detail', 'order_id BIGINT, tier STRING, net_amount BIGINT')
  + table('LogicalSummary', 'summary', 'tier STRING, total_amount BIGINT, order_count BIGINT')
  + table('SetLeft', 'set_left', 'v BIGINT, tag STRING')
  + table('SetRight', 'set_right', 'v BIGINT, tag STRING')
  + table('IntersectionSink', 'intersection', 'v BIGINT, tag STRING')
  + table('IntersectionAllSink', 'intersection_all', 'v BIGINT, tag STRING')
  + table('DifferenceSink', 'difference', 'v BIGINT, tag STRING')
  + table('DifferenceAllSink', 'difference_all', 'v BIGINT, tag STRING')
  + `CREATE TEMPORARY VIEW Enriched AS SELECT o.order_id, c.tier, o.amount + o.fee AS net_amount
FROM LogicalOrders o JOIN LogicalCustomers c ON o.customer_id = c.customer_id WHERE c.tier <> 'blocked';\n`;
const statements = `STATEMENT SET BEGIN
INSERT INTO LogicalDetail SELECT order_id, tier, net_amount FROM Enriched;
INSERT INTO LogicalSummary SELECT tier, SUM(net_amount), COUNT(order_id) FROM Enriched GROUP BY tier;
INSERT INTO IntersectionSink SELECT v, tag FROM SetLeft INTERSECT SELECT v, tag FROM SetRight;
INSERT INTO IntersectionAllSink SELECT v, tag FROM SetLeft INTERSECT ALL SELECT v, tag FROM SetRight;
INSERT INTO DifferenceSink SELECT v, tag FROM SetLeft EXCEPT SELECT v, tag FROM SetRight;
INSERT INTO DifferenceAllSink SELECT v, tag FROM SetLeft EXCEPT ALL SELECT v, tag FROM SetRight;
END;\n`;
function run(mode, sql) {
  const dir = path.join(root, mode);
  fs.mkdirSync(dir);
  fs.writeFileSync(path.join(dir, 'query.sql'), prelude(dir) + sql);
  const fd = fs.openSync(path.join(dir, 'sql-client.log'), 'w');
  let result;
  try {
    result = spawnSync('bash', [path.join(flink, 'bin/sql-client.sh'), '-f', path.join(dir, 'query.sql')],
      {cwd: dir, env: process.env, stdio: ['ignore', fd, fd], timeout: 180000});
  } finally { fs.closeSync(fd); }
  assert.ifError(result.error);
  assert.equal(result.status, 0, dir);
  assert.ok(!fs.readFileSync(path.join(dir, 'sql-client.log'), 'utf8').includes('[ERROR]'), dir);
  return dir;
}
const control = ['orders.customer_id:INDIRECT', 'customers.customer_id:INDIRECT', 'customers.tier:INDIRECT'];
const expected = {
  detail: {order_id: ['orders.order_id:DIRECT', ...control], tier: ['customers.tier:DIRECT', ...control],
    net_amount: ['orders.amount:DIRECT', 'orders.fee:DIRECT', ...control]},
  summary: {tier: ['customers.tier:DIRECT', ...control], total_amount: ['orders.amount:DIRECT', 'orders.fee:DIRECT', ...control],
    order_count: ['orders.order_id:DIRECT', ...control]}
};
const membership = ['set_left.v:INDIRECT', 'set_left.tag:INDIRECT', 'set_right.v:INDIRECT', 'set_right.tag:INDIRECT'];
for (const name of ['intersection', 'intersection_all']) expected[name] = {
  v: ['set_left.v:DIRECT', 'set_right.v:DIRECT', ...membership],
  tag: ['set_left.tag:DIRECT', 'set_right.tag:DIRECT', ...membership]
};
for (const name of ['difference', 'difference_all']) expected[name] = {
  v: ['set_left.v:DIRECT', ...membership], tag: ['set_left.tag:DIRECT', ...membership]
};
function verify(dir) {
  const rows = {detail: psql(`SELECT * FROM ${schema}.detail ORDER BY order_id`), summary: psql(`SELECT * FROM ${schema}.summary ORDER BY tier`)};
  for (const name of ['intersection', 'intersection_all', 'difference', 'difference_all']) {
    rows[name] = psql(`SELECT COALESCE(v::text,'NULL') || '|' || tag FROM ${schema}.${name} ORDER BY 1`);
  }
  fs.writeFileSync(path.join(dir, 'rows.json'), JSON.stringify(rows, null, 2));
  assert.deepEqual(rows, {detail: '1|gold|105\n3|gold|55', summary: 'gold|160|2',
    intersection: '1|a\nNULL|z', intersection_all: '1|a\n1|a\nNULL|z',
    difference: '2|b\n3|c', difference_all: '1|a\n2|b\n3|c'});
  const events = fs.readFileSync(path.join(dir, 'events.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  const starts = events.filter(e => e.eventType === 'START');
  const ends = events.filter(e => e.eventType === 'COMPLETE');
  assert.equal(starts.length, 1); assert.equal(ends.length, 1);
  assert.equal(starts[0].run.runId, ends[0].run.runId);
  for (const event of [starts[0], ends[0]]) {
    assert.equal(event.run.facets.flink_lineage.tableStatus, 'COMPLETE');
    assert.equal(event.run.facets.flink_lineage.columnStatus, 'COMPLETE');
  }
  // START carries the client-side graph. COMPLETE is a runtime status snapshot,
  // correlated by run ID, not a second publication of the graph.
  const event = starts[0];
  assert.deepEqual(event.inputs.map(d => d.name).sort(), ['customers', 'orders', 'set_left', 'set_right'].map(identity));
  assert.deepEqual(event.outputs.map(d => d.name).sort(), Object.keys(expected).map(identity).sort());
  for (const d of [...event.inputs, ...event.outputs]) assert.equal(d.namespace, namespace);
  const pairs = event.job.facets.lineage.entries.flatMap(o => o.inputs.map(i => {
    assert.equal(o.namespace, namespace); assert.equal(i.namespace, namespace);
    return i.name + '->' + o.name;
  })).sort();
  const expectedPairs = ['customers', 'orders'].flatMap(i => ['detail', 'summary'].map(o => identity(i) + '->' + identity(o)));
  expectedPairs.push(...['set_left', 'set_right'].flatMap(i => ['intersection', 'intersection_all', 'difference', 'difference_all'].map(o => identity(i) + '->' + identity(o))));
  assert.deepEqual(pairs, expectedPairs.sort());
  for (const [tableName, fields] of Object.entries(expected)) {
    const actual = event.outputs.find(o => o.name === identity(tableName)).facets.columnLineage.fields;
    assert.deepEqual(Object.keys(actual).sort(), Object.keys(fields).sort());
    for (const [field, references] of Object.entries(fields)) {
      const inputs = actual[field].inputFields.flatMap(i => {
        assert.equal(i.namespace, namespace);
        assert.ok(event.inputs.some(d => d.name === i.name && d.namespace === i.namespace));
        return i.transformations.map(t => i.name.slice(('lineage_poc.' + schema + '.').length) + '.' + i.field + ':' + t.type);
      });
      assert.deepEqual(inputs.sort(), [...references].sort(), tableName + '.' + field);
    }
  }
  console.log('PASS: ' + path.basename(dir) + ' PostgreSQL rows, physical identities, exact fields and lifecycle');
  return starts[0].run.runId;
}
const directId = verify(run('direct', ddl + 'EXECUTE ' + statements));
const plan = path.join(root, 'plan.json');
const compile = run('compile', ddl + `COMPILE PLAN '${plan}' FOR ` + statements);
assert.ok(!fs.existsSync(path.join(compile, 'events.jsonl')));
// Only this run's newly created output tables are cleared before restoring the same plan.
psql(`TRUNCATE ${Object.keys(expected).map(t => schema + '.' + t).join(',')};`);
const restoredId = verify(run('restored', `EXECUTE PLAN '${plan}';`));
assert.notEqual(restoredId, directId);
fs.writeFileSync(path.join(root, 'summary.json'), JSON.stringify({schema, namespace, directId, restoredId, passed: true}, null, 2));
console.log('PASS: fresh-process JDBC compiled-plan restore without original DDL');
