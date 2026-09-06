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

Lineage follows one automatic observer policy. The paired Flink build isolates
lineage extraction, binding, connector metadata and graph-validation failures:
otherwise valid jobs continue, and an invalid column-lineage bundle is discarded
as a whole. Known runtime source and sink inventories remain visible when they
can be recovered. Actual SQL validation, connector construction and execution
failures still fail the job.

Lifecycle events continue with `run.facets.flink_lineage`, independently of
metadata completeness:

| Property | Values | Meaning |
| --- | --- | --- |
| `tableStatus` | `COMPLETE`, `PARTIAL`, `UNAVAILABLE` | Verified logical table dependencies, only known runtime tables, or unavailable table metadata |
| `columnStatus` | `COMPLETE`, `UNAVAILABLE` | Verified complete column bundle or no column facet |
| `issues` | Array of strings | Reasons for incomplete or unavailable metadata |

For a valid plan whose optional lineage is missing, successful START/COMPLETE
events can carry `tableStatus=PARTIAL`, `columnStatus=UNAVAILABLE` and diagnostic
issues. These events retain known table inventories, but omit column facets and
the precise `job.facets.lineage` mapping. If even table capture or OpenLineage
conversion fails, the listener emits an unavailable observation without dataset
claims. `COMPLETE` in an event's type describes job execution; it does not upgrade
the lineage statuses.

Submission-side START and JobManager terminal events can use different listener
instances. This POC derives the OpenLineage run UUID deterministically from the
Flink JobID, so the same JobID identifies the same run on both sides. A snapshot
of lineage completeness is carried in internal job configuration and exposed
through `DefaultJobExecutionStatusEvent.getLineageStatus()` for the terminal
listener. This correlates lifecycle identity and metadata status; it does not
guarantee event delivery. The combined targeted suite passed 44 tests, and the
fresh Kubernetes Session and Application runs verified matching START/COMPLETE
run IDs and completeness snapshots on 2026-09-06.

Successful submission does not guarantee lineage availability or transport
delivery. Performance optimization and benchmarking are outside this POC's scope;
this isolation does not promise recovery from fatal JVM/resource failures.

### Exact table pairs in emitted events

For observations with `tableStatus=COMPLETE`, the Flink 2 converter emits
`job.facets.lineage.entries` from `LineageGraph.relations()`. Each output dataset
lists only its actual logical input datasets: independent StatementSet branches
`A -> X` and `B -> Y` do not
become `A -> Y` or `B -> X`. Repeated edges are deduplicated, and edges into the
same output are merged by namespace and name. A verified source-free sink has an
entry with an empty input list; incomplete observations never infer source-free
semantics from missing edges. Dataset identifier visitors and namespace resolution
are shared with the event's input/output datasets.

The existing top-level `inputs` and `outputs` remain available for compatibility.
Consumers must read the lineage facet to preserve precise table pairs; support
in Marquez storage, APIs and UI has **not** been verified. An absent/empty graph
does not manufacture a complete bipartite mapping. An edge whose dataset visitor
produces no identifier fails conversion instead of silently dropping that edge;
the listener records unavailable lineage and continues lifecycle emission.

In Marquez's [pinned event model at commit 180f37b](https://github.com/MarquezProject/marquez/blob/180f37b22387146187af1ef0279e3ee1d1ccd789/api/src/main/java/marquez/service/models/LineageEvent.java#L591-L608),
`ColumnLineageOutputColumn` declares `inputFields` and the legacy output-wide
`transformationDescription`/`transformationType` fields. Its
`ColumnLineageInputField` declares only `namespace`, `name` and `field`, with no
per-input `transformations` member. This is a source-model observation for that
revision, not proof of runtime ingestion, persistence, API behavior or current
UI display. Preserving both dependency roles in emitted events therefore does
not establish that Marquez preserves or displays them.

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
missing field identifies unavailable lineage. The execution plan remains usable;
recompile to obtain lineage. Invalid optional lineage is discarded without
relaxing validation of executable plan fields or malformed plan JSON.

Pruned-source snapshots contain the resolved schema and connector options,
including definitions of temporary tables. Protect compiled plans as sensitive
artifacts. These plans require `table.plan.compile.catalog-objects=ALL` (the
default) to retain lineage; `SCHEMA` and `IDENTIFIER` policies are respected and
complete lineage is unavailable when snapshots cannot be captured. Snapshotting
supports source, input-format and legacy source-function providers. Providers
that require executing a DataStream/transformation to
obtain their identity, or ambiguous definitions for the same pruned table,
remain explicit lineage errors, not execution vetoes. A missing lineage source
or invalid lineage field invalidates the complete column bundle and precise
logical table mapping; recovered runtime tables are only `PARTIAL`. A snapshot
is not a general fallback for missing metadata.

Output-wide transformation descriptions use the standard
`fields.<field>.transformationDescription` property. This property is deprecated
in the OpenLineage schema but preserves the scope of Flink's current metadata,
including generated fields with no inputs. Input fields are grouped by dataset
namespace, dataset name and field name. If a field has both `DIRECT` and
`INDIRECT` roles, one `inputFields` entry contains both transformation types;
repeated roles are deduplicated. These roles do not claim per-edge transformation
subtypes that Flink's model does not provide.

### SQL coverage and precision boundary

This is coverage of the paired branch's unoptimized logical-plan extractor,
not a claim to support every Flink SQL feature.

| SQL or planner shape | Current column-lineage behavior |
| --- | --- |
| Projection, aliases, Calc, casts, scalar expressions, CASE, constants | Supported; CASE conditions are indirect dependencies |
| Scalar UDF calls | Tracks supplied argument dependencies, not UDF internals or external reads |
| Filters, ordinary two-sided joins, grouping and aggregates | Supported; predicates and grouping retain indirect roles |
| OVER/Window, Union, sort/limit, Values | Supported for handled logical node and expression shapes |
| Views and nonrecursive CTEs | Expanded to underlying source-to-sink dependencies |
| Watermark assignment | Passes through column values without adding watermark-expression dependencies |
| Field selection from an explicit `ROW(a, b)` constructor | Tracks the selected component exactly through projections |
| Nested source ROW fields or black-box ROW result members | Explicitly unsupported; no top-level approximation is called precise nested lineage |
| RexSubQuery, correlated references, Correlate/UDTF/TableFunctionScan, Match | Explicitly unsupported |
| Intersect/Minus, recursive RepeatUnion/TableSpool, SEMI/ANTI joins | Unsupported by the current extraction path |

Member access after operations that do not preserve explicit ROW component
metadata remains unsupported. Unsupported extraction invalidates the column
bundle and reports the issue while the valid Flink job continues. The paired
extractor's batch and streaming suites passed 72 tests on 2026-09-06, including
red/green checks for Watermark and projected ROW selection; these are planner
tests, not deployed SQL Client or external-service acceptance.

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
successful execution with explicit unavailable column-lineage status for a
streaming plan with its column-lineage metadata removed. Expected
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
Its current checks cover direct execution, successful execution with incomplete
lineage and explicit status, and complex batch restore in a fresh SQL Client
process without original table/view definitions. The fixture requires a
MultipleInput node; its small CSV inputs select NestedLoopJoin, not
AdaptiveJoin. No optimizer setting is disabled to bypass serialization failures.
Follow the linked build instructions to avoid stale classes in incremental shaded Jars.
On 2026-09-06, a freshly packaged distribution and adapter were exercised through
SQL Client against an Operator-managed Kubernetes Session cluster, and through
Application mode. Direct execution, incomplete-lineage execution and saved-plan
restore all passed exact CSV, HTTP event, status and applicable dependency checks.
This is separate from rerunning the local-file-transport script above.
The image was `flink-lineage-local:observer-20260906`, with digest
`sha256:b4948949265a2514a33fba8f8b3bd75b3cd657516b6633eb265ab38a41e1ca73`.
Local evidence is retained under `build/observer-poc-20260906/evidence/`, including
`summary.json`, `verification.txt`, raw events, SQL, rows and job logs.
Two transport-isolation checks also passed: an unavailable client endpoint
prevented START delivery while the healthy JobManager still delivered terminal
events; a complete Collector outage produced zero received events while the job
finished with exact CSV results. The Collector was restored afterwards. The first
outage attempt was inconclusive because a TaskManager could not be scheduled;
the archived successful rerun used a fresh job after releasing this POC's completed
Application resources. Delivery is therefore not guaranteed by successful execution.
These are bounded filesystem/CSV POC checks, not full SQL/connector coverage,
SQL Gateway, checkpoint/savepoint or Marquez runtime/UI acceptance.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
