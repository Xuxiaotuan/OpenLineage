# OpenLineage Flink

The OpenLineage Flink integration uses JVM instrumentation to emit OpenLineage metadata.

Please refer to [https://openlineage.io/docs/integrations/flink](https://openlineage.io/docs/integrations/flink)
for more details.

## Native column lineage in this branch

This development branch pairs with the modified Flink `2.4-SNAPSHOT` build that
provides `LineageGraph.columnRelations()`. Install that Flink build and its test
artifacts into the local Maven repository before building this integration.
The defaults in `gradle.properties` select the paired version; no version
overrides are required:

```shell
./gradlew :flink2:test \
  --tests '*LineageGraphConverterTest' \
  --tests '*ColumnLineageRuntimeBoundaryTest' \
  --tests '*ColumnLineageStatementSetE2ETest' \
  verifyFlink2ColumnLineageJar
```

The resulting `build/libs/openlineage-flink-1.54.0-SNAPSHOT.jar` requires the
modified Flink distribution. Configure the job-status listener and transport
in the process submitting the SQL job, including SQL Client or SQL Gateway.
Deployment through those entry points and external connectors still requires
validation; the automated end-to-end tests use MiniCluster and file transport.

Flink rejects incomplete lineage before submission. Successful submission does
not guarantee transport delivery: the existing listener reports transport
failures without rolling back the submitted job.

### Exact table pairs in emitted events

The Flink 2 converter also emits `job.facets.lineage.entries` from
`LineageGraph.relations()`. Each output dataset lists only its actual logical
input datasets: independent StatementSet branches `A -> X` and `B -> Y` do not
become `A -> Y` or `B -> X`. Repeated edges are deduplicated, and edges into the
same output are merged by namespace and name. Dataset identifier visitors and
namespace resolution are shared with the event's input/output datasets.

The existing top-level `inputs` and `outputs` remain available for compatibility.
Consumers must read the lineage facet to preserve precise table pairs; support
in Marquez storage, APIs and UI has **not** been verified. An absent/empty graph
does not manufacture a complete bipartite mapping. An edge whose dataset visitor
produces no identifier fails conversion instead of silently dropping that edge;
the listener's existing transport/conversion error policy is unchanged.

Regression tests check independent branches with real sink rows for direct and
compiled-plan execution in MiniCluster, and verify that the same input field's
DIRECT and INDIRECT roles both survive event serialization. These tests use
Values and file transport, not Marquez or production connectors. This change
does not add exact expression provenance, origin propagation or reliable delivery.

If optimization removes a source entirely (for example, `WHERE 1=0`), Flink
retains its logical table and column dependencies using a frozen source snapshot.
The source is not added back to the execution topology. These events describe
SQL dependencies, not proof that every listed input was physically read.
The MiniCluster suite also checks real sink rows for a restored StatementSet
containing both a pruned branch and a live branch.

The paired Flink build now requires the `prunedSources` field in compiled
column-lineage metadata. An empty list proves that no source was pruned; a
missing field identifies an older plan and fails translation with a request to
recompile. Do not edit old plan JSON to bypass this check.

Pruned-source snapshots contain the resolved schema and connector options,
including definitions of temporary tables. Protect compiled plans as sensitive
artifacts. These plans require `table.plan.compile.catalog-objects=ALL` (the
default); `SCHEMA` and `IDENTIFIER` policies are rejected rather than silently
overridden. Snapshotting supports source, input-format and legacy source-function
providers. Providers that require executing a DataStream/transformation to
obtain their identity, or ambiguous definitions for the same pruned table,
remain explicit errors. A missing live source or invalid source field still
fails submission; a snapshot is not a general fallback for missing metadata.

Output-wide transformation descriptions use the standard
`fields.<field>.transformationDescription` property. This property is deprecated
in the OpenLineage schema but preserves the scope of Flink's current metadata,
including generated fields with no inputs. Dependency edges carry `DIRECT` or
`INDIRECT`; they do not claim per-edge transformation subtypes that Flink's model
does not provide.

### Long SQL session acceptance fixture

`flink2/src/test/resources/column-lineage/long-session.sql` contains a synthetic
commerce session: catalog/database selection, a SQL-registered two-argument UDF,
JSON_VALUE and CAST, three temporary views, filtering, a customer join and two
StatementSet outputs. `ColumnLineageLongSessionE2ETest` supplies six orders and
three customers, registers an in-memory catalog, sets Asia/Shanghai through the
Table API, substitutes test data IDs and executes the statements in order.
The marker-based loader is specific to this fixture, not a general SQL Client
or SQL Gateway script parser. The two INSERT statements are added to one
StatementSet before execution.

Run from this directory after installing the paired Flink artifacts:

```shell
./gradlew :flink2:test --tests '*ColumnLineageLongSessionE2ETest'
```

The five tests check direct batch execution, direct streaming execution,
batch and streaming compiled-plan restore from disk after dropping all three temporary views, and
rejection of a streaming plan with its column-lineage metadata removed. Expected
data and field dependencies are hand-derived, not generated from the converter.
The detail sink must contain `(1, gold, 105)`, `(4, silver, 305)` and
`(6, gold, 55)`; final summary rows must be `(gold, 160, 2)` and
`(silver, 305, 1)`. All six output fields are checked for exact DIRECT/INDIRECT
input sets. The summary sink accepts changelog updates in streaming mode.

Successful per-path events are retained under
`flink2/build/long-session-lineage/{direct-batch,direct-stream,restored-batch,restored-stream}.events.jsonl`;
the streaming and batch plans are saved alongside them as `compiled-plan.json`
and `compiled-batch-plan.json`.
These are synthetic test artifacts, not production data or a deployable UDF Jar.

**Batch restore repair (2026-09-05):** the paired Flink source now registers
`BatchExecAdaptiveJoin` with versioned metadata and JSON serialization/restore
support. Before this repair the batch restore test failed with `Missing type`.
Rebuild and install that paired Planner before running these tests; an older
2.4-SNAPSHOT artifact without the repair is insufficient. The batch test asserts
that the saved plan still contains `batch-exec-adaptive-join_1`, then restores it
from disk and checks actual data and all field dependencies. No adaptive optimizer
setting is disabled. This is compiled-plan recovery, not checkpoint/savepoint recovery.

This verifies flattened source-to-sink dependencies, not separate temporary-view
nodes. JSON_VALUE operates on a STRING payload; this does not establish support
for nested ROW field access, arbitrary UDF internals, external UDF Jar loading,
real Kafka/Paimon connectors, remote transport delivery or SQL Gateway deployment.

### SQL Client distribution acceptance

The opt-in [SQL Client acceptance script](flink2/src/test/scripts/sql-client-lineage/README.md)
uses a real paired distribution, the adapter in `lib`, and filesystem/CSV tables.
It retains exact data and field-lineage checks rather than trusting process exit status.
All three checks passed with the paired Flink `BatchExecMultipleInput` persistence
repair: direct execution, incomplete-lineage rejection, and complex batch restore
in a fresh SQL Client process without original table/view definitions. The fixture
requires a MultipleInput node; its small CSV inputs select NestedLoopJoin, not
AdaptiveJoin. No optimizer setting is disabled to bypass serialization failures.
Follow the linked build instructions to avoid stale classes in incremental shaded Jars.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
