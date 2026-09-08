# OpenLineage Flink

The OpenLineage Flink integration uses JVM instrumentation to emit OpenLineage metadata.

Please refer to [https://openlineage.io/docs/integrations/flink](https://openlineage.io/docs/integrations/flink)
for more details.

## Native column lineage in this branch

This is the Flink 2.2 backport branch. Dated results below describe earlier
2.4 development runs; they are not acceptance evidence for this backport.
The runtime and restore checks must use the paired 2.2 artifacts.

### POC fixes and delivery boundary (2026-09-08)

The paired 2.2 fix adds whole-row INDIRECT dependencies for `UNION DISTINCT`;
`UNION ALL` continues to merge only corresponding value origins and existing
row dependencies. The REST client also deletes a partially serialized plan when
serialization fails, preserving the original failure (and any cleanup failure
as a suppressed exception).

There is one enabled observation policy, not a strict/best-effort mode selector.
The emergency switch `table.lineage.enabled` defaults to `true`. Set it to
`false` in the submitting TableEnvironment/SQL session to skip the new logical
extraction and binding. Sink translation also ignores native column/logical-table
metadata in an already compiled plan while the switch is off. Existing runtime
source/sink inventory, listener lifecycle events and submission identity remain;
this switch does **not** turn the custom distribution into stock Flink, suppress
all events, or erase metadata from a previously saved plan.

Flink 2 event delivery now uses a single ordered, bounded queue (1024 waiting
events) per adapter classloader. Listener callbacks still construct events
synchronously, but no longer wait for transport I/O. A slow transport can delay
other queued deliveries, not make the callback wait for that transport. Queue
rejection is logged with run ID and event type and never uses caller-runs or
blocking enqueue. The sender is a daemon and exits after 30 idle seconds.

Delivery is still **best effort**, not a durable outbox: queue overflow, process
exit, exhausted transport retries or permanent transport errors can lose events.
There is no adapter replay after START failure or a listener/JM restart. Duplicate
terminal callbacks remain suppressed even if the first delivery fails. Transport
timeouts/retries are transport-specific; HTTP defaults to a 5000 ms timeout, not
a universal total-delivery deadline. Keep short-lived submitting processes alive
until the collector has received START; returning from submission is not a
delivery acknowledgement. `completedJobs` still has no retention bound and is a
known long-running Session/Gateway limitation, outside this short-lived POC.

`COMPLETE` describes validated logical dependencies within the supported Planner
model. It does not mean physical per-record provenance, UDF-body inspection,
independent temporary-view nodes, or reliable event delivery.

Remote CI inspected for the previous 2.2 commits: Flink `20cff962` run
[34145275978](https://github.com/Xuxiaotuan/flink/actions/runs/34145275978)
passed compile, basic QA, core, connect, misc, packaging and two E2E groups, but
failed table, tests and Python groups. The public annotations only expose exit
code 1; root causes have not been established. OpenLineage `41ef7620` workflows
were skipped. These are neither the old 2.4 CI result nor proof that the new
fix commits have green full CI. Local targeted tests must not be reported as
full CI, K8s reacceptance, HA/savepoint compatibility, or production approval.

Local verification for these fixes: 164 targeted Flink tests (22 REST client,
100 Batch/Streaming extractor, 42 propagation tests); 109 OpenLineage Flink 2
tests and the shaded-Jar boundary check. The new MiniCluster test runs eight
UNION DISTINCT combinations (Batch/Streaming, direct/restored, enabled/disabled),
checks exact duplicate/NULL sink rows and exact field dependency roles. Fault
tests cover slow sends, concurrent job callbacks, full-queue rejection and a
real HTTP collector rejecting START followed by successful later delivery.
These tests establish the documented loss boundary, not reliable replay.

Earlier backport validation on 2026-09-08: the Flink 2.2 native-lineage targeted tests
passed (67 runtime/client tests and 193 Planner tests, including focused reruns),
as did all 105 tests in this integration's `:flink2:test` suite and
`verifyFlink2ColumnLineageJar`. The rebuilt 2.2 distribution also passed the
SQL Client direct, incomplete/legacy metadata, mixed-sink and fresh-process
restore checks. PostgreSQL direct and restored execution passed exact row,
physical-identity, 20 table-edge and 22 output-field dependency assertions.
These checks do not establish arbitrary SQL, cross-version plan compatibility,
HA or production readiness. Kubernetes validation is tracked separately.

This development branch pairs with the modified Flink `2.2-SNAPSHOT` build that
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
otherwise valid jobs continue. Column lineage is retained only for output datasets
whose sink writers are all validated; an unsupported writer does not erase
independently validated columns for another output dataset. Known runtime source
and sink inventories remain visible when they
can be recovered. Actual SQL validation, connector construction and execution
failures still fail the job.

Lifecycle events continue with `run.facets.flink_lineage`, independently of
metadata completeness:

| Property | Values | Meaning |
| --- | --- | --- |
| `tableStatus` | `COMPLETE`, `PARTIAL`, `UNAVAILABLE` | Verified logical table dependencies, only known runtime tables, or unavailable table metadata |
| `columnStatus` | `COMPLETE`, `PARTIAL`, `UNAVAILABLE` | All, some, or none of the output datasets have validated complete column lineage |
| `columnStatuses` | Namespace to dataset-name to status map | Per-output-dataset `COMPLETE` or `UNAVAILABLE`; multiple writers are judged together |
| `tableStatuses` | Namespace to dataset-name to status map | Independent logical table coverage for each output; all its writers must be verified |
| `issues` | Array of strings | Reasons for incomplete or unavailable metadata |

`columnStatuses` and `tableStatuses` use native Flink dataset namespace/name keys so submission and
JobManager snapshots remain consistent. Dataset visitors may rename the standard
OpenLineage `outputs` identifiers; do not assume those renamed identifiers are the
keys of this native diagnostic map.

Table completeness is checked independently against the captured logical table
dependencies, not inferred from column completeness or runtime topology alone.
Thus `tableStatus=COMPLETE` with `columnStatus=UNAVAILABLE` still publishes precise
table pairs. If independent table evidence is missing too, START/COMPLETE can
carry `tableStatus=PARTIAL`, `columnStatus=UNAVAILABLE` and diagnostic issues;
known table inventories remain. Precise table pairs are emitted only for outputs
whose `tableStatuses` entry is `COMPLETE`, even if the overall status is `PARTIAL`.
If no output has independent complete table evidence, the precise facet is omitted.
If even table capture or OpenLineage
conversion fails, the listener emits an unavailable observation without dataset
claims. `COMPLETE` in an event's type describes job execution; it does not upgrade
the lineage statuses.

Submission-side START and JobManager terminal events can use different listener
instances. This POC derives the OpenLineage run UUID deterministically from the
pair `(Flink JobID, submissionId)`, so both sides identify the same submission.
The paired Flink build generates an internal submission ID before each
`RestClusterClient.submitJob`, `MiniCluster.submitJob`, or Embedded executor
submission, including repeated calls with the same plan object. It snapshots the
submitted plan and captures the ID for the client notification. REST plan
serialization now finishes within the submission call; upload and request
completion remain asynchronous, and serialization failures are returned through
the submission future. Transport retries reuse the same ID; persisted JobManager
recovery preserves its configuration,
and an Embedded HA reconnect does not create a new submission. Calling submit
again is a new submission attempt, not an internal transport retry. This does not
change Flink's existing rejection of duplicate JobIDs in a cluster. A snapshot
of lineage completeness is carried in internal job configuration and exposed
through `DefaultJobExecutionStatusEvent.getLineageStatus()` for the terminal
listener. This correlates lifecycle identity and metadata status; it does not
guarantee event delivery. The combined targeted suite passed 44 tests, and the
fresh Kubernetes Session and Application runs verified matching START/COMPLETE
run IDs and completeness snapshots on 2026-09-06. Those results predate the
per-dataset observation changes and are not acceptance evidence for this revision.

Listener state and completed-event suppression are isolated by `(JobID, submissionId)`.
Late events from an earlier submission cannot remove or update the later submission.
Only JobCreated produces START; INITIALIZING does not synthesize a second,
metadata-free START. COMPLETE, FAIL
and ABORT stop checkpoint tracking even if event construction or transport fails.
This is listener-local duplicate suppression, not distributed exactly-once delivery.
The adapter requires the paired custom Flink build containing `SubmissionIdentity`;
this is not binary compatibility with older or stock Flink distributions.
Legacy constructors and events without a submission ID in that paired build
retain JobID-only correlation and cannot distinguish reuse of a fixed JobID. The paired custom
stack supplies identity on both created and runtime status events; mixed
identified and legacy events cannot be correlated reliably.

Successful submission does not guarantee lineage availability or transport
delivery. Performance optimization and benchmarking are outside this POC's scope;
this isolation does not promise recovery from fatal JVM/resource failures.

### Exact table pairs in emitted events

For independently complete outputs, the Flink 2 converter emits
`job.facets.lineage.entries` from `LineageGraph.relations()`. Each output dataset
lists only its actual logical input datasets: independent StatementSet branches
`A -> X` and `B -> Y` do not
become `A -> Y` or `B -> X`. Repeated edges are deduplicated, and edges into the
same output are merged by namespace and name. A verified source-free sink has an
entry with an empty input list; incomplete observations never infer source-free
semantics from missing edges. Dataset identifier visitors and namespace resolution
are shared with the event's input/output datasets.
If complete and incomplete native outputs resolve to the same OpenLineage
identifier, the shared resolved output is excluded from exact table claims.
This prevents identifier aliases from hiding an incomplete writer.

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
uses metadata only: an already available lineage vertex provider supplies identity;
otherwise the snapshot explicitly uses a logical catalog namespace. It does not
construct a runtime provider or execute a DataStream/transformation to discover
identity. Recovered snapshots preserve their recorded namespace, including
historical snapshots. `COMPLETE` does not certify that a logical dataset identity
has been verified against a physical system. Ambiguous definitions for the same
pruned table remain explicit lineage errors, not execution vetoes. A missing lineage source
or invalid lineage field makes the affected output dataset's column lineage
unavailable. Independently validated logical table metadata is preserved; when
that evidence is missing, recovered runtime tables are only `PARTIAL`. A snapshot
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
| INTERSECT / EXCEPT, DISTINCT and ALL | Whole-row membership is indirect; INTERSECT merges positional value origins, EXCEPT returns values only from the first input |
| SEMI / ANTI relational Join nodes | Only left value fields are returned; join predicates and both inputs' row filters remain indirect dependencies |
| Views and nonrecursive CTEs | Expanded to underlying source-to-sink dependencies |
| Watermark assignment | Passes through column values without adding watermark-expression dependencies |
| Field selection from an explicit `ROW(a, b)` constructor | Tracks the selected component exactly through projections |
| Nested source ROW fields or black-box ROW result members | Explicitly unsupported; no top-level approximation is called precise nested lineage |
| IN / NOT IN / EXISTS / NOT EXISTS expressions | Exact value/control dependencies for the tested projection, WHERE, correlation and JOIN ON shapes; see the current revision notes below |
| Scalar subqueries, Correlate/UDTF/TableFunctionScan, Match | Explicitly unsupported |
| Recursive RepeatUnion/TableSpool | Unsupported by the current extraction path |

SEMI/ANTI node support alone does not establish support for SQL subqueries:
the capture point precedes subquery rewriting, so membership expressions and
their correlation scopes require separate extraction and tests. Membership set operations use the
existing FILTER transformation with exact DIRECT/INDIRECT field roles; they do
not change Flink's execution plan or add a serialized transformation enum value.
The mixed-sink negative fixture now uses a scalar MIN subquery instead of EXISTS,
so it continues testing unavailable columns alongside independently valid outputs.

Member access after operations that do not preserve explicit ROW component
metadata remains unsupported. Unsupported extraction invalidates the affected
output dataset's column bundle and reports the issue while the valid Flink job
continues; unrelated validated output datasets retain their column facets. The paired
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
2.2-SNAPSHOT artifact without the repair is insufficient. The batch test asserts
that the saved plan still contains `batch-exec-adaptive-join_1`, then restores it
from disk and checks actual data and all field dependencies. No adaptive optimizer
setting is disabled. This is compiled-plan recovery, not checkpoint/savepoint recovery.

This verifies flattened source-to-sink dependencies, not separate temporary-view
nodes. JSON_VALUE operates on a STRING payload; this does not establish support
for nested ROW field access, arbitrary UDF internals, external UDF Jar loading,
real Kafka/Paimon connectors, remote transport delivery or SQL Gateway deployment.

### SQL Client distribution acceptance

#### Membership subquery revision (2026-09-07, Asia/Shanghai)

The paired Planner suites passed 139 tests: 49 batch, 49 streaming and 41
propagation/recovery tests. The extractor handles declared correlation scopes
and tested JOIN ON correlations against the combined left/right row, preserving
already bound ancestors. It does not infer ownership from matching field types.
Deeply nested JOIN scopes that cannot be resolved are explicitly rejected by
column extraction; the observer isolates that failure rather than changing the
Flink execution plan. This does not claim arbitrary nested ON support. Two
ambiguous nested examples are also rejected by Flink's own SQL explain path;
other deeper combinations may conservatively lose column lineage and remain
outside this verified coverage.

The current worktree adds exact membership checks to the JDBC acceptance:
four physical inputs, ten outputs, twenty table edges and twenty-two output
fields. IN and EXISTS preserve matching left duplicates; the right-side NULL
makes NOT IN empty, while NOT EXISTS retains the unmatched and NULL left rows.
An unused EXISTS SELECT expression must not become a field dependency.
Both direct execution and fresh-process compiled-plan restore passed the same
literal row and field-role assertions using the final rebuilt distribution.
Raw PostgreSQL evidence is retained at
`/var/folders/c0/wzxnynsn36vf746bn4vcx_9r0000gn/T/ol-jdbc-4Bky1M/`.
The paired adapter passed 105 tests, Spotless and the Jar verifier.
All seven SQL Client distribution cases also passed, including missing metadata,
mixed sinks and their independent-process restores. Raw evidence is at
`/var/folders/c0/wzxnynsn36vf746bn4vcx_9r0000gn/T/ol-cli-NJMKMJ/`.

The Operator fixture generator now replaces the uniquely named Unsupported
writer and rejects missing or duplicate writers. Its positive SQL also exercises
the four membership predicates. The new image
`flink-lineage-local:subquery-20260907` passed ten real Kubernetes cases on
isolated `subquery-*` resources: direct, incomplete metadata, legacy metadata,
mixed sinks, mixed restore, partial-table restore, complex restore, cancellation,
runtime failure and Application. Each check correlates actual remote JobID,
expected terminal state, received HTTP events and applicable literal data/field
assertions. The first direct-case harness wait expired after 60 seconds while
the job was progressing; bounded polling then verified that same JobID without
resubmission. Both logs remain available. This is not a performance result.
The completed new Session Deployment was scaled to zero to fit Application;
its evidence PVC is retained, and pre-existing resources were not changed.
No collector-outage rerun, HA or reliable-delivery claim is included.

The Planner change is committed as Flink `3f3590df8c1`. The tested artifacts were
built immediately before that commit, from `f56bb1c0120` plus the recorded source
diff; provenance correctly records a dirty build, not a clean base-commit build.
The image, all distribution library hashes, source diffs, manifests and runtime
evidence are retained under `integration/flink/build/subquery-poc-20260907/`.

The independent SQL Gateway executor-recovery test race was fixed separately in
Flink `f56bb1c01200ea04866618583288c21ec7612447`. Its focused test and all 33 tests
in the class passed locally. The push-triggered
[CI run](https://github.com/Xuxiaotuan/flink/actions/runs/34107566540)
passed basic QA and compilation before being superseded and cancelled by the
subsequent Planner push. The
[final-revision run](https://github.com/Xuxiaotuan/flink/actions/runs/34109274497)
is still running; this is not a full-CI success claim.

#### Set operations and real PostgreSQL POC (2026-09-07, Asia/Shanghai)

The opt-in [JDBC acceptance script](flink2/src/test/scripts/jdbc-lineage/README.md)
uses a real isolated PostgreSQL container, deliberately different logical aliases
and physical table names, and one six-sink StatementSet. It checks JOIN, filters,
aggregation, INTERSECT / EXCEPT with DISTINCT and ALL, duplicates and NULLs.
Its assertions require four physical inputs, six outputs, twelve exact table
edges and fourteen output fields with exact DIRECT/INDIRECT references, plus
literal expected database rows. The same checks run after a fresh-process saved
plan restore without the original DDL. START carries the graph; COMPLETE carries
the correlated runtime status snapshot, not a repeated graph payload.

These changes are local worktree changes based on Flink `c895962138fb` and
OpenLineage `f2a0a5be688b`; those commits alone do not identify the new build.
The focused Flink batch/stream extraction and propagation suites passed 117 tests,
including a red/green guard against incorrectly retaining left-only ROW member
metadata after INTERSECT. This does not establish general SQL subquery support.

The final rebuilt distribution passed both direct PostgreSQL execution and
fresh-process plan restore, including the strengthened duplicate-multiplicity
fixture. Local raw evidence is retained at
`/var/folders/c0/wzxnynsn36vf746bn4vcx_9r0000gn/T/ol-jdbc-P667xF/`.
Build provenance (explicitly marked dirty), distribution library hashes and the
acceptance log are under `integration/flink/build/jdbc-poc-20260907/` from the
repository root. The paired adapter passed 105 tests, Spotless and the distribution
Jar verifier. PostgreSQL is the dedicated `lineage-jdbc-poc-20260907` container,
published only at `127.0.0.1:32768`; generated schemas and evidence are retained.
The same final distribution also passed all seven SQL Client positive/negative
acceptance cases, including independent metadata loss and mixed-sink restores;
raw evidence is at
`/var/folders/c0/wzxnynsn36vf746bn4vcx_9r0000gn/T/ol-cli-4PfKag/`.

At the time of this set-operation acceptance, no new Kubernetes image or Operator
rollout was included. The then-existing Operator `prepare-cases.cjs` partial-table
fixture replaced the old INTERSECT SQL literal and still needed synchronization
with the correlated-EXISTS negative fixture. Historical Kubernetes
results below are not evidence for this new worktree.
The prior-commit remote CI run `34096566618` failed in
`SqlGatewayServiceITCase.testReleaseLockWhenFailedToSubmitOperation` with
`RejectedExecutionException` on a saturated executor. That separate test failure
was not fixed by that set-operation revision; these focused POC passes do not claim full CI is green.

#### Latest submission-identity acceptance (2026-09-07, Asia/Shanghai)

The fresh `submission-*` Kubernetes resources ran clean Flink
`c895962138fbf3ed92a2c07a8968081f09832adb` with OpenLineage
`f5ab758f182a79a102d242159459f103623929bd`. Image
`flink-lineage-local:submission-20260907` has ID
`sha256:4056814d8274646093a929ba04abb6ecd8e25c3765281cc69fdface34b0f56f6`;
the adapter SHA-256 is
`e4b065260442df8c3a0efa8fc560fc7e3e70790cf2e4d94cc9dd3fd13e793d5b`.
The distribution was rebuilt with forced Jar creation; provenance and hashes of
all 14 distribution-lib Jars are retained with the raw evidence.

All ten cases passed exact data and applicable table/column relationships,
remote job-state checks, and matching START/terminal run IDs: direct submission,
column metadata loss, legacy metadata loss, mixed sinks, mixed restore,
partial-table restore, complex batch restore, cancellation, runtime CAST failure,
and Operator Application mode. The complex restore job was
`a5fbff12f5599c13762086742d9a2b04`; the Application job was
`0ffa4b7e3b1ebc17e965739b895139a7`. Expected cancellation and failure are verified
negative cases, not successful business jobs.

A separate complete Collector outage left job
`33cbe02256e05964f14edcde16c5e67e` FINISHED with exact CSV rows and zero received
events. The event count stayed at 29, and the Collector was restored to 1/1.
This proves execution isolation, not reliable delivery. Only the new completed
Session was scaled to zero; its evidence PVC remains and older resources were
not changed. Session JobManager memory was 1 GiB to fit the local test node.

Evidence is local at `build/submission-poc-20260907/`, including `summary.json`,
manifests, SQL, events, output rows, logs, build provenance and library hashes.
The paired local suites passed 105 adapter tests, 182 Planner tests, 26 client
tests and 8 Sink recovery tests before this deployment. A subsequent JavaDoc-only
format repair passed Checkstyle and Spotless across all six changed Flink modules.
The fresh [remote CI run](https://github.com/Xuxiaotuan/flink/actions/runs/34096566618)
is separate evidence; deployment success does not establish that all CI jobs pass.
This matrix does not exercise fixed-JobID resubmission, live JobManager HA,
network retry, savepoints, real Kafka/JDBC/Paimon services, or unsupported SQL.

#### Verified compatibility and isolation repair (2026-09-07, Asia/Shanghai)

The image `flink-lineage-local:repaired-20260907` was built from clean Flink
`392397175622eb797a92164b78e4c825d2ed139e` and OpenLineage
`6751f8e0679df6e21370f9e8807be1a9a6d7e950` checkouts. Image ID:
`sha256:2f35de180d750a2ec8d3741dd761b5ca11801e4ef954b0f91b30f5cfcd6da9bd`.
Adapter SHA-256:
`1c83d37f93a513b801a8cae1776588d0018641b247cf4ffde4df3a1d33e804e9`.

All ten fresh Kubernetes cases passed: direct execution, column-only metadata
loss, legacy metadata loss, mixed sinks, mixed restore, partial-table restore,
complex batch restore, cancellation, runtime CAST failure, and Application mode.
The new partial-table case removes metadata only from one independent writer:
both outputs' rows remain correct, Good retains exact table and column
relations, and Unsupported is UNAVAILABLE in both native status maps. The job
lineage statuses remain PARTIAL and no Unsupported table entry is fabricated.

Full Collector outage was rerun on this image. Job
`f28066fe160282a34870f831a4164ea6` reached FINISHED with exact result rows and
zero received events; the Collector was restored to 1/1 afterwards. This proves
execution isolation, not guaranteed lineage delivery.

Regression evidence includes 237 planner tests, 42 runtime/streaming tests,
61 Flink 2 integration tests plus 5 shared tracker tests, 4 Python compiled-plan
tests, and 4 event-verifier tests. The 28 updated plan snapshots were structurally
compared with their previous versions: only optional lineage blocks changed.
Eight graph fixtures now check full table and column JSON rather than replacing
the graph with statuses. The fresh full
[Flink CI run](https://github.com/Xuxiaotuan/flink/actions/runs/34078937122)
is separate from these local checks.

Local raw evidence is retained under `build/repaired-poc-20260907/`: clean build
provenance, all 14 distribution-library hashes, image metadata, manifests,
ten actual JobIDs and remote states, exact CSV/HTTP verification, outage
endpoints/events, and SQL/JobManager logs. Previous `lineage-*` resources were not
modified; completed owned session resources were scaled down with their PVCs
retained. These checks do not establish arbitrary SQL/connector support, HA,
savepoint compatibility, Marquez consumption, or production readiness.

#### Verified independent-observation revision (2026-09-07, Asia/Shanghai)

The fresh local Kubernetes image `flink-lineage-local:isolated-20260907` was built
from clean Flink `fe22cf714bb7ab567fe13cf0ad9abb834e34da42` and OpenLineage
`a85ea412d2eb2afedfaf31c6dca04e9dd2e8a25b` source checkouts. Its image ID is
`sha256:a32bdb5bf45715385c586fbe9409110071fa46c7ac814dacdfb7fc0ca92de94b`.
The adapter SHA-256 is
`f54fab26c5a23b85887c84ee64bda0e1b33318bffe0d77ee654774bb5ebd7cd1`.

All nine remote checks passed: direct SQL, column-only metadata loss, legacy
table-and-column metadata loss, mixed supported/unsupported sinks, mixed saved
plan restore, complex batch restore, cancellation, runtime CAST failure and
Application mode. Successful jobs require exact CSV rows and HTTP lineage;
cancellation/failure require actual CANCELED/FAILED job states and matching
ABORT/FAIL events. Mixed sinks retain three exact table pairs and only the
supported output's column facet, with identical per-output statuses at START
and COMPLETE. Unsupported columns do not veto execution.

The revision passed 82 targeted Flink tests and 63 targeted OpenLineage tests,
including 12 MiniCluster boundary/StatementSet/long-session tests. These are
targeted regressions, not full upstream CI. Fresh-build evidence is local under
`build/isolated-poc-20260907/`: `build-provenance.json`, all 14 distribution-lib
hashes, image metadata, manifests, and `evidence/results.json`,
`evidence/verification.txt`, raw events, data and logs. Initial cancellation SQL
used an unquoted reserved identifier and failed before submission; its log is
retained alongside the corrected successful run. Collector outage was not
rerun on this image; the 2026-09-06 outage evidence below remains historical.

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
