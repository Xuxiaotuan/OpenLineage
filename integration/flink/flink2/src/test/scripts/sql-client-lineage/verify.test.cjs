// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const test = require('node:test');
const verify = require('./verify.cjs');

test('complete acceptance preserves direct and indirect roles on the same input field', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ol-complete-verifier-'));
  for (const [sink, content] of Object.entries({detail: '1,gold,105\n3,gold,55\n', summary: 'gold,160,2\n'})) {
    fs.mkdirSync(path.join(root, sink));
    fs.writeFileSync(path.join(root, sink, 'part-0'), content);
  }
  const namespace = 'flink://catalog/default_catalog';
  const name = table => '`default_catalog`.`lineage_acceptance`.`' + table + '`';
  const indirect = ['Orders.customer_id', 'Customers.customer_id', 'Customers.tier'];
  const fields = direct => {
    const merged = new Map();
    for (const [type, columns] of [['DIRECT', direct], ['INDIRECT', indirect]]) {
      for (const column of columns) {
        const [table, field] = column.split('.');
        if (!merged.has(column)) merged.set(column, {namespace, name: name(table), field, transformations: []});
        merged.get(column).transformations.push({type});
      }
    }
    return {inputFields: [...merged.values()]};
  };
  const output = (table, definitions) => ({namespace, name: name(table), facets: {columnLineage: {
    fields: Object.fromEntries(Object.entries(definitions).map(([field, direct]) => [field, fields(direct)]))
  }}});
  const run = {runId: 'complete-run', facets: {flink_lineage: {tableStatus: 'COMPLETE', columnStatus: 'COMPLETE', issues: []}}};
  const start = {eventType: 'START', run,
    job: {facets: {lineage: {entries: ['Detail', 'Summary'].map(table => ({
      namespace, name: name(table), inputs: ['Orders', 'Customers'].map(input => ({namespace, name: name(input)}))
    }))}}},
    inputs: ['Orders', 'Customers'].map(table => ({namespace, name: name(table)})),
    outputs: [
      output('Detail', {order_id: ['Orders.order_id'], tier: ['Customers.tier'], net_amount: ['Orders.amount', 'Orders.fee']}),
      output('Summary', {tier: ['Customers.tier'], total_amount: ['Orders.amount', 'Orders.fee'], order_count: ['Orders.order_id']})
    ]};
  fs.writeFileSync(path.join(root, 'events.jsonl'), [start, {eventType: 'COMPLETE', run, inputs: [], outputs: []}].map(JSON.stringify).join('\n'));
  assert.doesNotThrow(() => verify(root));
});

test('incomplete acceptance requires data, completion and honest lineage status', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ol-incomplete-verifier-'));
  fs.mkdirSync(path.join(root, 'detail'));
  fs.writeFileSync(path.join(root, 'detail', 'part-0'), '1,fixed,105\n2,fixed,205\n3,fixed,55\n');
  const events = ['START', 'COMPLETE'].map(eventType => ({
    eventType,
    run: {runId: 'same-run', facets: {flink_lineage: {
      tableStatus: 'PARTIAL', columnStatus: 'UNAVAILABLE', issues: ['missing column lineage']
    }}},
    inputs: eventType === 'START' ? [{name: '`default_catalog`.`lineage_acceptance`.`Orders`'}] : [],
    outputs: eventType === 'START' ? [{name: '`default_catalog`.`lineage_acceptance`.`Detail`', facets: {}}] : []
  }));
  const save = () => fs.writeFileSync(path.join(root, 'events.jsonl'), events.map(JSON.stringify).join('\n'));
  save();
  assert.doesNotThrow(() => verify.incomplete(root));
  events[1].eventType = 'FAIL';
  save();
  assert.throws(() => verify.incomplete(root), /must complete/);
  events[1].eventType = 'COMPLETE';
  events[0].run.facets.flink_lineage.columnStatus = 'COMPLETE';
  save();
  assert.throws(() => verify.incomplete(root));
  events[0].run.facets.flink_lineage.columnStatus = 'UNAVAILABLE';
  events[0].outputs[0].facets.columnLineage = {fields: {}};
  save();
  assert.throws(() => verify.incomplete(root), /No column facet/);
  console.log('Verifier test artifacts: ' + root);
});
