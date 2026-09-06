// Copyright 2018-2026 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const assert = require('node:assert/strict');
const test = require('node:test');
const verify = require('./verify.cjs');

test('lifecycle acceptance distinguishes cancellation and failure without losing lineage status', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ol-lifecycle-verifier-'));
  const run = {runId:'terminal-run',facets:{flink_lineage:{tableStatus:'COMPLETE',columnStatus:'COMPLETE',issues:[]}}};
  const events = [{eventType:'START',run},{eventType:'ABORT',run}];
  const save = () => fs.writeFileSync(path.join(root,'events.jsonl'),events.map(JSON.stringify).join('\n'));
  save();
  assert.doesNotThrow(() => verify.terminal(root,'ABORT'));
  assert.throws(() => verify.terminal(root,'FAIL'));
  events[1].eventType='FAIL';
  save();
  assert.doesNotThrow(() => verify.terminal(root,'FAIL'));
  events.push({eventType:'COMPLETE',run});
  save();
  assert.throws(() => verify.terminal(root,'FAIL'), /Exactly one terminal/);
});

test('mixed sink acceptance retains only supported columns and rejects false complete claims', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ol-mixed-verifier-'));
  for (const [sink, content] of Object.entries({good: '2\n3\n4\n', unsupported: '2\n3\n'})) {
    fs.mkdirSync(path.join(root, sink));
    fs.writeFileSync(path.join(root, sink, 'part-0'), content);
  }
  const namespace = 'flink://catalog/default_catalog';
  const name = table => '`default_catalog`.`lineage_acceptance`.`' + table + '`';
  const status = {tableStatus: 'COMPLETE', columnStatus: 'PARTIAL', issues: ['unsupported INTERSECT'],
    columnStatuses: {[namespace]: {[name('Good')]: 'COMPLETE', [name('Unsupported')]: 'UNAVAILABLE'}}};
  const run = {runId: 'mixed-run', facets: {flink_lineage: status}};
  const events = [{eventType: 'START', run,
    job: {facets: {lineage: {entries: [
      {namespace, name: name('Good'), inputs: [{namespace, name: name('Numbers')}]},
      {namespace, name: name('Unsupported'), inputs: ['Numbers','OtherNumbers'].map(t=>({namespace,name:name(t)}))}
    ]}}},
    inputs: ['Numbers','OtherNumbers'].map(t=>({namespace,name:name(t)})),
    outputs: [{namespace,name:name('Good'),facets:{columnLineage:{fields:{value:{inputFields:[{
      namespace,name:name('Numbers'),field:'value',transformations:[{type:'DIRECT'}]
    }]}}}}}, {namespace,name:name('Unsupported'),facets:{}}]
  }, {eventType:'COMPLETE',run,inputs:[],outputs:[]}];
  const save = () => fs.writeFileSync(path.join(root,'events.jsonl'),events.map(JSON.stringify).join('\n'));
  save();
  assert.doesNotThrow(() => verify.mixed(root));
  events[0].outputs[1].facets.columnLineage = {fields:{}};
  save();
  assert.throws(() => verify.mixed(root), /Unsupported sink/);
});

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
      tableStatus: 'COMPLETE', columnStatus: 'UNAVAILABLE', issues: ['missing column lineage']
    }}},
    job: {facets: eventType === 'START' ? {lineage: {entries:[{
      namespace:'flink://catalog/default_catalog',name:'`default_catalog`.`lineage_acceptance`.`Detail`',
      inputs:[{namespace:'flink://catalog/default_catalog',name:'`default_catalog`.`lineage_acceptance`.`Orders`'}]
    }]}} : {}},
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
  delete events[0].outputs[0].facets.columnLineage;
  for (const event of events) event.run.facets.flink_lineage.tableStatus = 'PARTIAL';
  save();
  assert.throws(() => verify.incomplete(root, true), /must not claim exact table edges/);
  delete events[0].job.facets.lineage;
  save();
  assert.doesNotThrow(() => verify.incomplete(root, true));
  console.log('Verifier test artifacts: ' + root);
});
