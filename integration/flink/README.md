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

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2026 contributors to the OpenLineage project
