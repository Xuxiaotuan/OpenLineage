// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
function verify(root) {
  function rows(dir) {
    return fs.readdirSync(dir, {withFileTypes: true}).flatMap(entry => {
      const file = path.join(dir, entry.name);
      if (entry.name.startsWith('.') || entry.name.startsWith('_')) return [];
      return entry.isDirectory() ? rows(file) : fs.readFileSync(file, 'utf8').trim().split(/\r?\n/).filter(Boolean);
    }).sort();
  }
  assert.deepEqual(rows(path.join(root, 'detail')), ['1,gold,105', '3,gold,55']);
  assert.deepEqual(rows(path.join(root, 'summary')), ['gold,160,2']);
  const events = fs.readFileSync(path.join(root, 'events.jsonl'), 'utf8').trim().split(/\r?\n/).map(JSON.parse);
  const starts = events.filter(e => e.eventType === 'START');
  assert.equal(starts.length, 1);
  const start = starts[0];
  assert.ok(start.run.runId);
  const identity = table => '`default_catalog`.`lineage_acceptance`.`' + table + '`';
  assert.deepEqual(start.inputs.map(i => i.name).sort(), ['Customers', 'Orders'].map(identity));
  assert.deepEqual(start.outputs.map(i => i.name).sort(), ['Detail', 'Summary'].map(identity));
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
        assert.equal(i.transformations.length, 1);
        const source = ['Orders', 'Customers'].find(t => identity(t) === i.name);
        return source + '.' + i.field + ':' + i.transformations[0].type;
      });
      assert.deepEqual(actual.sort(), [...direct, ...indirect].sort(), table + '.' + field);
    }
  }
  return start.outputs.map(o => ({name: o.name, fields: o.facets.columnLineage.fields})).sort((a, b) => a.name.localeCompare(b.name));
}
module.exports = verify;
if (require.main === module) {
  verify(process.argv[2]);
  console.log('PASS: real CSV outputs and six exact field dependency sets');
}
