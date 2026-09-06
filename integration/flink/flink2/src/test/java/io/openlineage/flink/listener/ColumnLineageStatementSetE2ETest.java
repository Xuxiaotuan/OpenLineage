/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.testutils.LineageTestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** End-to-end contract for native Flink column lineage and OpenLineage conversion. */
class ColumnLineageStatementSetE2ETest {

  private static final ObjectMapper OBJECT_MAPPER = OpenLineageClientUtils.newObjectMapper();
  private static final Path EVENTS_FILE =
      Path.of("build", "task-6-column-lineage-events", "events.jsonl");

  private static final String REVENUE_INSERT =
      "INSERT INTO RevenueSink "
          + "SELECT o.region, SUM(o.amount * 2) "
          + "FROM Orders AS o JOIN Customers AS c "
          + "ON o.customer_id = c.customer_id "
          + "WHERE c.tier <> 'blocked' "
          + "GROUP BY o.region";

  private static final String TIER_INSERT =
      "INSERT INTO TierSink "
          + "SELECT c.tier, COUNT(o.order_id) "
          + "FROM Orders AS o JOIN Customers AS c "
          + "ON o.customer_id = c.customer_id "
          + "GROUP BY c.tier";

  @RegisterExtension
  private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setConfiguration(createConfiguration())
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  @BeforeEach
  void clearEvents() throws Exception {
    Files.createDirectories(EVENTS_FILE.getParent());
    Files.deleteIfExists(EVENTS_FILE);
  }

  @Test
  void emitsCompleteOpenLineageColumnLineageJsonForTwoSinkStatementSet() throws Exception {
    TableEnvironmentImpl environment = createEnvironment();

    createStatementSet(environment).execute().await(30, TimeUnit.SECONDS);

    assertThat(Files.exists(EVENTS_FILE)).isTrue();
    List<RunEvent> events = LineageTestUtils.fromFile(EVENTS_FILE.toString());
    RunEvent startEvent =
        events.stream()
            .filter(event -> event.getEventType() == EventType.START)
            .filter(event -> event.getOutputs() != null && event.getOutputs().size() == 2)
            .findFirst()
            .orElseThrow(
                () -> new AssertionError("No two-sink OpenLineage START event was emitted"));

    assertThat(columnLineageJson(startEvent.getOutputs()))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "[{\"namespace\":\"flink://catalog/default_catalog\","
                    + "\"name\":\"`default_catalog`.`default_database`.`RevenueSink`\","
                    + "\"columnLineage\":{\"fields\":{"
                    + "\"region\":{\"transformationDescription\":\"GROUP_BY,JOIN,FILTER\",\"inputFields\":["
                    + input("Orders", "region", "DIRECT", "INDIRECT")
                    + ","
                    + input("Orders", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "tier", "INDIRECT")
                    + "]},"
                    + "\"revenue\":{\"transformationDescription\":\"EXPRESSION,AGGREGATION,JOIN,FILTER,GROUP_BY\",\"inputFields\":["
                    + input("Orders", "amount", "DIRECT")
                    + ","
                    + input("Orders", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "tier", "INDIRECT")
                    + ","
                    + input("Orders", "region", "INDIRECT")
                    + "]}}}},"
                    + "{\"namespace\":\"flink://catalog/default_catalog\","
                    + "\"name\":\"`default_catalog`.`default_database`.`TierSink`\","
                    + "\"columnLineage\":{\"fields\":{"
                    + "\"tier\":{\"transformationDescription\":\"GROUP_BY,JOIN\",\"inputFields\":["
                    + input("Customers", "tier", "DIRECT", "INDIRECT")
                    + ","
                    + input("Orders", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "customer_id", "INDIRECT")
                    + "]},"
                    + "\"order_count\":{\"transformationDescription\":\"AGGREGATION,JOIN,GROUP_BY\",\"inputFields\":["
                    + input("Orders", "order_id", "DIRECT")
                    + ","
                    + input("Orders", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "customer_id", "INDIRECT")
                    + ","
                    + input("Customers", "tier", "INDIRECT")
                    + "]}}}}]"));
  }

  @Test
  void emitsMergedLineageForRestoredSameSinkStatementSet() throws Exception {
    TableEnvironmentImpl environment = createEnvironment();
    StatementSet statements = environment.createStatementSet();
    statements.addInsertSql("INSERT INTO RevenueSink SELECT region, amount FROM Orders");
    statements.addInsertSql("INSERT INTO RevenueSink SELECT tier, customer_id FROM Customers");
    CompiledPlan plan = statements.compilePlan();
    environment
        .loadPlan(PlanReference.fromJsonString(plan.asJsonString()))
        .execute()
        .await(30, TimeUnit.SECONDS);

    RunEvent start =
        LineageTestUtils.fromFile(EVENTS_FILE.toString()).stream()
            .filter(event -> event.getEventType() == EventType.START)
            .findFirst()
            .orElseThrow();
    assertThat(start.getOutputs()).hasSize(1);
    JsonNode fields =
        columnLineageJson(start.getOutputs()).get(0).path("columnLineage").path("fields");
    assertThat(fields.size()).isEqualTo(2);
    assertThat(fields.path("region").path("inputFields"))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "["
                    + input("Orders", "region", "DIRECT")
                    + ","
                    + input("Customers", "tier", "DIRECT")
                    + "]"));
    assertThat(fields.path("revenue").path("inputFields"))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "["
                    + input("Orders", "amount", "DIRECT")
                    + ","
                    + input("Customers", "customer_id", "DIRECT")
                    + "]"));
    assertThat(fields.path("region").path("transformationDescription").asText()).contains("UNION");
    assertThat(fields.path("revenue").path("transformationDescription").asText()).contains("UNION");
  }

  @Test
  void retainsPrunedSourceLineageWithoutWritingItsRows() throws Exception {
    TableEnvironmentImpl environment = createEnvironment();
    String prunedData = TestValuesTableFactory.registerData(Collections.singletonList(Row.of(99L)));
    String liveData = TestValuesTableFactory.registerData(Collections.singletonList(Row.of(7L)));
    Schema schema = Schema.newBuilder().column("value", DataTypes.BIGINT()).build();
    environment.createTemporaryTable(
        "PrunedSource",
        TableDescriptor.forConnector("values")
            .schema(schema)
            .option("bounded", "true")
            .option("data-id", prunedData)
            .build());
    environment.createTemporaryTable(
        "LiveSource",
        TableDescriptor.forConnector("values")
            .schema(schema)
            .option("bounded", "true")
            .option("data-id", liveData)
            .build());
    environment.createTemporaryTable(
        "CollectedSink", TableDescriptor.forConnector("values").schema(schema).build());
    StatementSet statements = environment.createStatementSet();
    statements.addInsertSql("INSERT INTO CollectedSink SELECT `value` FROM PrunedSource WHERE 1=0");
    statements.addInsertSql("INSERT INTO CollectedSink SELECT `value` FROM LiveSource");
    String plan = statements.compilePlan().asJsonString();
    environment.dropTemporaryTable("PrunedSource");
    environment.loadPlan(PlanReference.fromJsonString(plan)).execute().await(30, TimeUnit.SECONDS);

    assertThat(TestValuesTableFactory.getResults("CollectedSink")).containsExactly(Row.of(7L));
    RunEvent start =
        LineageTestUtils.fromFile(EVENTS_FILE.toString()).stream()
            .filter(event -> event.getEventType() == EventType.START)
            .findFirst()
            .orElseThrow();
    assertThat(start.getInputs())
        .extracting(input -> input.getName())
        .containsExactlyInAnyOrder(
            "`default_catalog`.`default_database`.`PrunedSource`",
            "`default_catalog`.`default_database`.`LiveSource`");
    JsonNode fields =
        columnLineageJson(start.getOutputs()).get(0).path("columnLineage").path("fields");
    assertThat(fields.path("value").path("inputFields"))
        .isEqualTo(
            OBJECT_MAPPER.readTree(
                "["
                    + input("PrunedSource", "value", "DIRECT")
                    + ","
                    + input("LiveSource", "value", "DIRECT")
                    + "]"));
  }

  @Test
  void incompleteLineageDoesNotPreventJobExecution(@InjectMiniCluster MiniCluster miniCluster)
      throws Exception {
    TableEnvironmentImpl environment = createEnvironment();
    CompiledPlan compiledPlan = createStatementSet(environment).compilePlan();
    JsonNode planJson = OBJECT_MAPPER.readTree(compiledPlan.asJsonString());

    assertThat(removeColumnLineage(planJson)).isEqualTo(2);
    int jobsBefore = miniCluster.listJobs().get().size();
    CompiledPlan legacyPlan =
        environment.loadPlan(
            PlanReference.fromJsonString(OBJECT_MAPPER.writeValueAsString(planJson)));

    assertThat(miniCluster.listJobs().get()).hasSize(jobsBefore);
    assertThat(Files.exists(EVENTS_FILE)).isFalse();

    legacyPlan.execute().await(30, TimeUnit.SECONDS);
    assertThat(miniCluster.listJobs().get()).hasSize(jobsBefore + 1);
    List<RunEvent> events = LineageTestUtils.fromFile(EVENTS_FILE.toString());
    assertThat(events)
        .extracting(RunEvent::getEventType)
        .contains(EventType.START, EventType.COMPLETE);
    for (RunEvent event : events) {
      JsonNode status =
          OBJECT_MAPPER.valueToTree(event).path("run").path("facets").path("flink_lineage");
      assertThat(status.path("columnStatus").asText()).isEqualTo("UNAVAILABLE");
      assertThat(status.path("issues").size()).isGreaterThan(0);
      if (event.getEventType() == EventType.START) {
        assertThat(event.getOutputs()).hasSize(2);
        assertThat(event.getOutputs())
            .allSatisfy(output -> assertThat(output.getFacets().getColumnLineage()).isNull());
      }
    }
  }

  @Test
  void independentTablePairsSurviveDirectSubmission() throws Exception {
    assertIndependentTablePairs(false);
  }

  @Test
  void independentTablePairsSurvivePlanRestore() throws Exception {
    assertIndependentTablePairs(true);
  }

  private void assertIndependentTablePairs(boolean restore) throws Exception {
    TableEnvironmentImpl environment = createEnvironment();
    Schema schema = Schema.newBuilder().column("a", DataTypes.BIGINT()).build();
    String[] sources = {"IndependentA", "IndependentB"};
    String[] sinks = {"IndependentX", "IndependentY"};
    StatementSet statements = environment.createStatementSet();
    for (int i = 0; i < 2; i++) {
      String data = TestValuesTableFactory.registerData(List.of(Row.of(11L + i)));
      environment.createTemporaryTable(
          sources[i],
          TableDescriptor.forConnector("values")
              .schema(schema)
              .option("bounded", "true")
              .option("data-id", data)
              .build());
      environment.createTemporaryTable(
          sinks[i], TableDescriptor.forConnector("values").schema(schema).build());
      statements.addInsertSql(
          "INSERT INTO " + sinks[i] + " SELECT a FROM " + sources[i] + " WHERE a > 0");
    }
    if (restore) {
      String plan = statements.compilePlan().asJsonString();
      environment
          .loadPlan(PlanReference.fromJsonString(plan))
          .execute()
          .await(30, TimeUnit.SECONDS);
    } else {
      statements.execute().await(30, TimeUnit.SECONDS);
    }
    assertThat(TestValuesTableFactory.getResults("IndependentX")).containsExactly(Row.of(11L));
    assertThat(TestValuesTableFactory.getResults("IndependentY")).containsExactly(Row.of(12L));
    RunEvent start =
        LineageTestUtils.fromFile(EVENTS_FILE.toString()).stream()
            .filter(e -> e.getEventType() == EventType.START)
            .findFirst()
            .orElseThrow();
    JsonNode entries =
        OBJECT_MAPPER.valueToTree(start.getJob().getFacets()).path("lineage").path("entries");
    assertThat(entries.size()).isEqualTo(2);
    for (int i = 0; i < 2; i++) {
      String target = "`default_catalog`.`default_database`.`" + sinks[i] + "`";
      JsonNode entry = null;
      for (JsonNode candidate : entries) {
        if (candidate.path("name").asText().equals(target)) {
          entry = candidate;
        }
      }
      assertThat(entry).isNotNull();
      assertThat(entry.path("namespace").asText()).isEqualTo("values://AppendingSinkFunction");
      assertThat(entry.path("inputs").size()).isEqualTo(1);
      assertThat(entry.path("inputs").get(0).path("namespace").asText())
          .isEqualTo("values://FromElementsFunction");
      assertThat(entry.path("inputs").get(0).path("name").asText())
          .isEqualTo("`default_catalog`.`default_database`.`" + sources[i] + "`");
      OutputDataset output =
          start.getOutputs().stream()
              .filter(o -> o.getName().equals(target))
              .findFirst()
              .orElseThrow();
      JsonNode fields = OBJECT_MAPPER.valueToTree(output.getFacets().getColumnLineage());
      assertThat(fields.path("fields").path("a").path("inputFields"))
          .isEqualTo(
              OBJECT_MAPPER.readTree("[" + input(sources[i], "a", "DIRECT", "INDIRECT") + "]"));
    }
  }

  private static StatementSet createStatementSet(TableEnvironmentImpl environment) {
    StatementSet statementSet = environment.createStatementSet();
    statementSet.addInsertSql(REVENUE_INSERT);
    statementSet.addInsertSql(TIER_INSERT);
    return statementSet;
  }

  private static TableEnvironmentImpl createEnvironment() {
    TableEnvironmentImpl environment =
        (TableEnvironmentImpl)
            TableEnvironmentImpl.create(
                EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .withConfiguration(createConfiguration())
                    .build());
    environment.createTemporaryTable(
        "Orders",
        TableDescriptor.forConnector("values")
            .schema(
                Schema.newBuilder()
                    .column("order_id", DataTypes.BIGINT())
                    .column("customer_id", DataTypes.BIGINT())
                    .column("amount", DataTypes.BIGINT())
                    .column("region", DataTypes.STRING())
                    .build())
            .option("bounded", "true")
            .build());
    environment.createTemporaryTable(
        "Customers",
        TableDescriptor.forConnector("values")
            .schema(
                Schema.newBuilder()
                    .column("customer_id", DataTypes.BIGINT())
                    .column("tier", DataTypes.STRING())
                    .build())
            .option("bounded", "true")
            .build());
    environment.createTemporaryTable(
        "RevenueSink",
        TableDescriptor.forConnector("blackhole")
            .schema(
                Schema.newBuilder()
                    .column("region", DataTypes.STRING())
                    .column("revenue", DataTypes.BIGINT())
                    .build())
            .build());
    environment.createTemporaryTable(
        "TierSink",
        TableDescriptor.forConnector("blackhole")
            .schema(
                Schema.newBuilder()
                    .column("tier", DataTypes.STRING())
                    .column("order_count", DataTypes.BIGINT())
                    .build())
            .build());
    return environment;
  }

  private static JsonNode columnLineageJson(List<OutputDataset> outputs) {
    ArrayNode result = OBJECT_MAPPER.createArrayNode();
    outputs.stream()
        .sorted(Comparator.comparing(OutputDataset::getName))
        .forEach(
            output -> {
              ObjectNode dataset = result.addObject();
              dataset.put("namespace", output.getNamespace());
              dataset.put("name", output.getName());
              ObjectNode columnLineage =
                  OBJECT_MAPPER.valueToTree(output.getFacets().getColumnLineage());
              columnLineage.remove(List.of("_producer", "_schemaURL"));
              dataset.set("columnLineage", columnLineage);
            });
    return result;
  }

  private static int removeColumnLineage(JsonNode node) {
    int removed = 0;
    if (node.isObject()) {
      removed += ((ObjectNode) node).remove("columnLineage") == null ? 0 : 1;
    }
    for (JsonNode child : node) {
      removed += removeColumnLineage(child);
    }
    return removed;
  }

  private static String input(String table, String field, String... dependencyTypes) {
    return "{\"namespace\":\"values://FromElementsFunction\","
        + "\"name\":\"`default_catalog`.`default_database`.`"
        + table
        + "`\",\"field\":\""
        + field
        + "\",\"transformations\":[{\"type\":\""
        + String.join("\"},{\"type\":\"", dependencyTypes)
        + "\"}]}";
  }

  private static Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set(
        JOB_STATUS_CHANGED_LISTENERS,
        Collections.singletonList(OpenLineageJobStatusChangedListenerFactory.class.getName()));
    configuration.setString("openlineage.transport.type", "file");
    configuration.setString("openlineage.transport.location", EVENTS_FILE.toString());
    configuration.setString("openlineage.flink.disableCheckpointTracking", "true");
    configuration.setString("parallelism.default", "1");
    return configuration;
  }
}
