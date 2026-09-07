<!-- Copyright 2018-2026 contributors to the OpenLineage project -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# PostgreSQL physical identity and set-operation acceptance

This opt-in test runs the real paired SQL Client against an isolated PostgreSQL
container. It is not a mocked metadata visitor test or Kubernetes/HA acceptance.
Use only a dedicated container named `lineage-jdbc-poc-*`, with database
`lineage_poc`, a `postgres` test user, and one published `127.0.0.1` port for 5432.
The script creates a fresh timestamped schema; it never touches existing schemas.
It clears only its own output tables between direct and restored execution.
The fixture uses a dummy password with local test-only trust authentication;
do not use this authentication configuration outside the isolated test container.

Requirements: Java 17, Node.js, Docker, the rebuilt paired Flink distribution and
adapter, plus JDBC core 4.0.0-2.0, JDBC PostgreSQL dialect 4.0.0-2.0, and PostgreSQL
driver 42.7.5 Jars in a separate directory. Connector dependencies follow the
[official JDBC documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/connectors/table/jdbc/).
These versions are test inputs, not a claim that every connector release is
compatible with this custom Flink branch. They are not added to Flink core.

```sh
node run.cjs /absolute/paired/flink /absolute/openlineage-flink.jar \
  /absolute/jdbc-jars lineage-jdbc-poc-example
```

The script copies the distribution into a fresh temporary directory and adds
the adapter and connector Jars to `lib`. It prints the evidence directory and
retains SQL, raw events, process logs, database result rows and the compiled plan.
No container or database cleanup is automated.

The same StatementSet exercises a filtered JOIN and aggregation plus INTERSECT,
INTERSECT ALL, EXCEPT and EXCEPT ALL. Duplicate rows, NULLs and equal keys with
different second-column values distinguish whole-row membership and multiplicity.
Checks require four physical source identities, six physical output identities,
twelve precise table edges, all fourteen output fields' exact DIRECT/INDIRECT
dependencies, exact PostgreSQL output rows, and matching START/COMPLETE run IDs.
The table aliases deliberately differ from physical table names. Every field
reference must match a published physical input identity and PostgreSQL namespace.
Exact graph assertions inspect START, which publishes the client-side graph.
COMPLETE is a runtime status snapshot rather than another graph publication;
it must match the run ID and report COMPLETE for both table and column status.
The shared `(1, 'a')` row occurs three times on the left and twice on the right:
INTERSECT returns it once, INTERSECT ALL twice, EXCEPT never, and EXCEPT ALL once.

The plan is then compiled without submitting, and restored in a fresh SQL Client
process without the original table/view DDL. The same assertions must pass; the
new submission must have a different run ID. Any missing or partial column
lineage fails this positive acceptance test.

This does not prove all SQL subqueries, streaming JDBC changelog behavior,
Kubernetes deployment, HA recovery, real network retries or reliable delivery.
