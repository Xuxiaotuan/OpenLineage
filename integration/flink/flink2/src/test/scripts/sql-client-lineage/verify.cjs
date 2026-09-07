// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
function rows(dir) {
    return fs.readdirSync(dir, {withFileTypes: true}).flatMap(entry => {
      const file = path.join(dir, entry.name);
      if (entry.name.startsWith('.') || entry.name.startsWith('_')) return [];
      return entry.isDirectory() ? rows(file) : fs.readFileSync(file, 'utf8').trim().split(/\r?\n/).filter(Boolean);
    }).sort();
}
function verify(root) {
  assert.deepEqual(rows(path.join(root, 'detail')), ['1,gold,105', '3,gold,55']);
  assert.deepEqual(rows(path.join(root, 'summary')), ['gold,160,2']);
  const events = fs.readFileSync(path.join(root, 'events.jsonl'), 'utf8').trim().split(/\r?\n/).map(JSON.parse);
  const starts = events.filter(e => e.eventType === 'START');
  assert.equal(starts.length, 1);
  const start = starts[0];
  assert.ok(start.run.runId);
  const completes = events.filter(e => e.eventType === 'COMPLETE');
  assert.equal(completes.length, 1, 'Successful execution must emit COMPLETE');
  assert.equal(completes[0].run.runId, start.run.runId);
  for (const event of [start, completes[0]]) {
    const status = event.run.facets.flink_lineage;
    assert.equal(status.tableStatus, 'COMPLETE');
    assert.equal(status.columnStatus, 'COMPLETE');
    assert.deepEqual(status.issues, []);
  }
  const identity = table => '`default_catalog`.`lineage_acceptance`.`' + table + '`';
  assert.deepEqual(start.inputs.map(i => i.name).sort(), ['Customers', 'Orders'].map(identity));
  assert.deepEqual(start.outputs.map(i => i.name).sort(), ['Detail', 'Summary'].map(identity));
  const pairs = start.job.facets.lineage.entries.flatMap(output => output.inputs.map(input => {
    assert.equal(output.namespace, 'flink://catalog/default_catalog');
    assert.equal(input.namespace, output.namespace);
    return input.name + ' -> ' + output.name;
  }));
  assert.deepEqual(pairs.sort(), ['Detail', 'Summary'].flatMap(output =>
    ['Orders', 'Customers'].map(input => identity(input) + ' -> ' + identity(output))).sort());
  const inputs = new Map(start.inputs.map(i => [i.name, i.namespace]));
  for (const dataset of [...start.inputs, ...start.outputs]) {
    assert.equal(dataset.namespace, 'flink://catalog/default_catalog');
  }
  const indirect = ['Orders.customer_id:INDIRECT', 'Customers.customer_id:INDIRECT', 'Customers.tier:INDIRECT'];
  const expected = {
    Detail: {order_id: ['Orders.order_id:DIRECT'], tier: ['Customers.tier:DIRECT'], net_amount: ['Orders.amount:DIRECT', 'Orders.fee:DIRECT']},
    Summary: {tier: ['Customers.tier:DIRECT'], total_amount: ['Orders.amount:DIRECT', 'Orders.fee:DIRECT'], order_count: ['Orders.order_id:DIRECT']}
  };
  for (const [table, fields] of Object.entries(expected)) {
    const output = start.outputs.find(o => o.name === identity(table));
    const actualFields = output.facets.columnLineage.fields;
    assert.deepEqual(Object.keys(actualFields).sort(), Object.keys(fields).sort());
    for (const [field, direct] of Object.entries(fields)) {
      const actual = actualFields[field].inputFields.flatMap(i => {
        assert.ok(inputs.has(i.name));
        assert.equal(i.namespace, inputs.get(i.name));
        const source = ['Orders', 'Customers'].find(t => identity(t) === i.name);
        assert.ok(i.transformations.length > 0);
        return i.transformations.map(t => source + '.' + i.field + ':' + t.type);
      });
      assert.deepEqual(actual.sort(), [...direct, ...indirect].sort(), table + '.' + field);
    }
  }
  return start.outputs.map(o => ({name: o.name, fields: o.facets.columnLineage.fields})).sort((a, b) => a.name.localeCompare(b.name));
}
module.exports = verify;
module.exports.terminal = function verifyTerminal(root, expectedType) {
  assert.ok(['FAIL', 'ABORT'].includes(expectedType));
  const events = fs.readFileSync(path.join(root, 'events.jsonl'), 'utf8').trim().split(/\r?\n/).map(JSON.parse);
  const starts = events.filter(e => e.eventType === 'START');
  const terminal = events.filter(e => ['FAIL', 'ABORT', 'COMPLETE'].includes(e.eventType));
  assert.equal(starts.length, 1);
  assert.equal(terminal.length, 1, 'Exactly one terminal event');
  assert.equal(terminal[0].eventType, expectedType);
  assert.equal(terminal[0].run.runId, starts[0].run.runId);
  const initial = starts[0].run.facets.flink_lineage;
  assert.equal(initial.tableStatus, 'COMPLETE');
  assert.equal(initial.columnStatus, 'COMPLETE');
  assert.deepEqual(initial.issues, []);
  assert.deepEqual(terminal[0].run.facets.flink_lineage, initial, 'Terminal status preserves lineage availability');
};
module.exports.mixed = function verifyMixed(root, partialTable = false) {
  assert.deepEqual(rows(path.join(root, 'good')), ['2', '3', '4']);
  assert.deepEqual(rows(path.join(root, 'unsupported')), partialTable ? ['3', '4', '5'] : ['2', '3']);
  const events = fs.readFileSync(path.join(root, 'events.jsonl'), 'utf8').trim().split(/\r?\n/).map(JSON.parse);
  const starts = events.filter(e => e.eventType === 'START');
  const terminal = events.filter(e => ['COMPLETE', 'ABORT', 'FAIL'].includes(e.eventType));
  assert.equal(starts.length, 1);
  assert.equal(terminal.length, 1);
  assert.equal(terminal[0].eventType, 'COMPLETE');
  assert.equal(terminal[0].run.runId, starts[0].run.runId);
  const namespace = 'flink://catalog/default_catalog';
  const name = table => '`default_catalog`.`lineage_acceptance`.`' + table + '`';
  for (const event of [starts[0], terminal[0]]) {
    const status = event.run.facets.flink_lineage;
    assert.equal(status.tableStatus, partialTable ? 'PARTIAL' : 'COMPLETE');
    assert.equal(status.columnStatus, 'PARTIAL');
    assert.ok(status.issues.length > 0);
    assert.deepEqual(status.columnStatuses, {[namespace]: {
      [name('Good')]: 'COMPLETE', [name('Unsupported')]: 'UNAVAILABLE'
    }});
    if (partialTable) assert.deepEqual(status.tableStatuses, {[namespace]: {
      [name('Good')]: 'COMPLETE', [name('Unsupported')]: 'UNAVAILABLE'
    }});
  }
  const start = starts[0];
  assert.deepEqual(terminal[0].run.facets.flink_lineage, start.run.facets.flink_lineage,
    'Terminal status preserves all per-sink availability');
  assert.deepEqual(start.inputs.map(i => i.name).sort(), [name('Numbers'), name('OtherNumbers')]);
  assert.deepEqual(start.outputs.map(o => o.name).sort(), [name('Good'), name('Unsupported')]);
  for (const dataset of [...start.inputs, ...start.outputs]) assert.equal(dataset.namespace, namespace);
  const pairs = start.job.facets.lineage.entries.flatMap(o => o.inputs.map(i => i.name + ' -> ' + o.name));
  const expectedPairs = [name('Numbers') + ' -> ' + name('Good')];
  if (!partialTable) expectedPairs.push(name('Numbers') + ' -> ' + name('Unsupported'),
    name('OtherNumbers') + ' -> ' + name('Unsupported'));
  assert.deepEqual(pairs.sort(), expectedPairs.sort(), 'Only verified exact table pairs');
  assert.deepEqual(start.job.facets.lineage.entries.map(o => o.name).sort(),
    (partialTable ? ['Good'] : ['Good', 'Unsupported']).map(name), 'Only verified output entries');
  const good = start.outputs.find(o => o.name === name('Good'));
  const unsupported = start.outputs.find(o => o.name === name('Unsupported'));
  assert.ok(!unsupported.facets?.columnLineage, 'Unsupported sink must not have a column facet');
  const fields = good.facets.columnLineage.fields;
  assert.deepEqual(Object.keys(fields), ['value']);
  assert.equal(fields.value.inputFields.length, 1);
  const input = fields.value.inputFields[0];
  assert.equal(input.namespace, namespace);
  assert.equal(input.name, name('Numbers'));
  assert.equal(input.field, 'value');
  assert.deepEqual(input.transformations.map(t => t.type), ['DIRECT']);
  return {fields, columnStatuses: start.run.facets.flink_lineage.columnStatuses};
};
module.exports.incomplete = function verifyIncomplete(root, legacy = false) {
  assert.deepEqual(rows(path.join(root, 'detail')), ['1,fixed,105', '2,fixed,205', '3,fixed,55']);
  const events = fs.readFileSync(path.join(root, 'events.jsonl'), 'utf8').trim().split(/\r?\n/).map(JSON.parse);
  const starts = events.filter(e => e.eventType === 'START');
  const completes = events.filter(e => e.eventType === 'COMPLETE');
  assert.equal(starts.length, 1, 'Exactly one submitted job');
  assert.equal(completes.length, 1, 'Valid job must complete despite missing lineage');
  assert.equal(completes[0].run.runId, starts[0].run.runId);
  for (const event of [starts[0], completes[0]]) {
    const status = event.run.facets.flink_lineage;
    assert.equal(status.tableStatus, legacy ? 'PARTIAL' : 'COMPLETE');
    assert.equal(status.columnStatus, 'UNAVAILABLE');
    assert.ok(status.issues.length > 0, 'Unavailable lineage must explain why');
  }
  assert.ok(starts[0].inputs.length > 0, 'Known source table remains visible');
  assert.ok(starts[0].outputs.length > 0, 'Known sink table remains visible');
  const identity = table => '`default_catalog`.`lineage_acceptance`.`' + table + '`';
  assert.deepEqual(starts[0].inputs.map(i => i.name), [identity('Orders')]);
  assert.deepEqual(starts[0].outputs.map(o => o.name), [identity('Detail')]);
  if (legacy) {
    assert.ok(!starts[0].job?.facets?.lineage, 'Partial table inventory must not claim exact table edges');
  } else {
    const entries = starts[0].job.facets.lineage.entries;
    assert.equal(entries.length, 1);
    assert.equal(entries[0].name, identity('Detail'));
    assert.equal(entries[0].namespace, 'flink://catalog/default_catalog');
    assert.deepEqual(entries[0].inputs.map(i => [i.namespace, i.name]),
      [['flink://catalog/default_catalog', identity('Orders')]]);
  }
  for (const output of starts[0].outputs) {
    assert.ok(!output.facets?.columnLineage, 'No column facet may claim completeness');
  }
};
if (require.main === module) {
  verify(process.argv[2]);
  console.log('PASS: real CSV outputs and six exact field dependency sets');
}
