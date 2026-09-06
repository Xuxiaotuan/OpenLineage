<!-- Copyright 2018-2026 contributors to the OpenLineage project -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL Client distribution acceptance

This is an explicit, opt-in acceptance script, not part of the Gradle unit tests.
It runs the real distribution's `bin/sql-client.sh` in local execution mode with
the OpenLineage adapter in `lib`. It does not use the test `values` connector.
It requires Node.js 18+ and the paired Flink build's supported Java (tested with Java 17).

Build the paired Flink distribution from its repository root:

```sh
./mvnw -pl flink-dist -am package -DskipTests -Dfast -Pskip-webui-build -Djar.forceCreation=true -T2
```

The force-creation flag matters for incremental shaded builds. An existing
Planner Bundle can otherwise retain stale Planner classes even when Maven
reports success. A clean build is another option; do not manually patch Jar contents.
This build command skips tests and is not evidence of the full Flink test suite passing.

Build the adapter with the paired local Maven artifacts, then run from
`integration/flink`:

```sh
./gradlew verifyFlink2ColumnLineageJar
node flink2/src/test/scripts/sql-client-lineage/run.cjs \
  /absolute/path/to/flink/flink-dist/target/flink-2.4-SNAPSHOT-bin/flink-2.4-SNAPSHOT \
  build/libs/openlineage-flink-1.54.0-SNAPSHOT.jar
```

Set `JAVA_HOME` to Java 17 for these commands. The script copies the distribution
into a fresh temporary directory, adds the adapter, and retains SQL, process
logs, CSV results, events and any saved plans for inspection. It prints the
directory at startup. It does not modify the supplied distribution or start a
persistent standalone cluster. Each SQL Client invocation has a 120-second limit.
The temporary distribution path intentionally avoids `flink-sql-client`:
the upstream launcher's classpath regex can mistake that directory name for
the SQL Client Jar and omit the actual Jar.

## Required checks and current status (2026-09-06)

The script implements the single automatic observer policy. The checks below
describe its current assertions. On 2026-09-06 the corresponding remote HTTP
acceptance checks passed against a freshly packaged distribution in Kubernetes:
direct Session submission, incomplete-lineage execution, saved-plan restore and
Application submission. Evidence is retained in
`integration/flink/build/observer-poc-20260906/evidence/` from the repository root.
This does not claim that this local-mode/file-transport runner was rerun; earlier
local PASS results used an obsolete rejection assertion.

1. **Direct batch submission:** two filesystem/CSV sources, a filtered Join,
   temporary view and two StatementSet sinks. Checks exact rows
   `1,gold,105`, `3,gold,55` and `gold,160,2`, one START and one COMPLETE event
   with the same run ID, two inputs, two outputs, all six field dependency sets,
   and dataset identity alignment. Both events require
   `run.facets.flink_lineage.tableStatus=COMPLETE`, `columnStatus=COMPLETE`
   and an empty `issues` list.
2. **Incomplete-lineage execution:** compiles a separate single-source projection,
   removes its one `columnLineage` property and attempts execution in another
   SQL Client process. Requires actual sink rows `1,fixed,105`, `2,fixed,205`
   and `3,fixed,55`, one START and one COMPLETE with the same run ID,
   `tableStatus=PARTIAL`, `columnStatus=UNAVAILABLE`, diagnostic `issues`, known
   input/output tables and no column facet. Missing optional lineage must not
   reject an otherwise valid job. Empty staging directories alone are not proof
   of execution; exact CSV rows are required.
3. **Complex batch plan restore:** compiles the original dual-sink query,
   drops its temporary view, and restores it in a fresh SQL Client process with
   no original table/view definitions. Requires a serialized MultipleInput node
   and exact data/lineage equality with direct execution.

The restore fixture requires the paired Flink MultipleInput subgraph persistence
repair. Before that repair, check 3 failed with `Missing type`. It requires more
than the earlier AdaptiveJoin repair: shared node identities and original input
edge references must survive restoration. No multiple-input or adaptive optimizer
setting is disabled. These small CSV inputs naturally choose NestedLoopJoin inside
MultipleInput; an earlier assertion incorrectly required AdaptiveJoin to appear.
AdaptiveJoin configuration being enabled does not guarantee that node is selected.
This fixture proves the MultipleInput path, not an AdaptiveJoin runtime path.
The script exits nonzero if any required data, lineage, lifecycle or completeness
status check fails.

The complete observation publishes precise table pairs through
`job.facets.lineage`; partial runtime table inventories do not establish those
pairs and omit that facet. If table capture or conversion is unavailable,
`tableStatus=UNAVAILABLE` and `columnStatus=UNAVAILABLE` describe the loss while
lifecycle emission continues. These statuses describe metadata, not job success
or confirmed transport delivery. Input-field DIRECT/INDIRECT roles share one
field entry when both apply. See the [integration coverage matrix](../../../../../README.md#sql-coverage-and-precision-boundary)
for supported and unsupported SQL shapes. Performance work is outside this POC.

The execution target is local, and the transport writes a local file.
This is not independent SQL Gateway, remote-cluster, Kafka/JDBC/Paimon,
checkpoint/savepoint or remote-delivery acceptance. Filesystem datasets use
Catalog-qualified identities here, not physical-file deduplication across catalogs.
