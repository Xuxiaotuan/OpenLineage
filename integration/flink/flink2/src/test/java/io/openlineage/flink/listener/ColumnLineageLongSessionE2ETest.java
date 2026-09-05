/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.testutils.LineageTestUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Real rows and exact flattened field dependencies through a multi-statement SQL session. */
public class ColumnLineageLongSessionE2ETest {
  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();
  private static final Path EVENTS = Path.of("build", "long-session-lineage", "events.jsonl");

  @RegisterExtension
  static final MiniClusterExtension CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setConfiguration(configuration())
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  @BeforeEach
  void clearEvents() throws Exception {
    Files.createDirectories(EVENTS.getParent());
    Files.deleteIfExists(EVENTS);
  }

  @Test
  void directBatchSessionPreservesViewUdfAndJsonDependencies() throws Exception {
    executeAndVerify(false, false);
  }

  @Test
  void directStreamingSessionPreservesViewUdfAndJsonDependencies() throws Exception {
    executeAndVerify(false, true);
  }

  @Test
  void restoredPlanPreservesDependenciesAfterViewsAreDropped() throws Exception {
    executeAndVerify(true, true);
  }

  @Test
  void restoredBatchPlanPreservesDependenciesAfterViewsAreDropped() throws Exception {
    executeAndVerify(true, false);
  }

  private void executeAndVerify(boolean restored, boolean streaming) throws Exception {
    TableEnvironment environment = environment(streaming);
    StatementSet statements = loadSession(environment);
    if (restored) {
      String plan = statements.compilePlan().asJsonString();
      if (!streaming) {
        assertThat(MAPPER.readTree(plan).findValuesAsText("type"))
            .contains("batch-exec-adaptive-join_1");
      }
      Path planFile =
          EVENTS.getParent().resolve(streaming ? "compiled-plan.json" : "compiled-batch-plan.json");
      Files.writeString(planFile, plan);
      assertThat(environment.dropTemporaryView("EnrichedOrders")).isTrue();
      assertThat(environment.dropTemporaryView("PaidOrders")).isTrue();
      assertThat(environment.dropTemporaryView("ParsedOrders")).isTrue();
      environment.loadPlan(PlanReference.fromFile(planFile)).execute().await(30, TimeUnit.SECONDS);
    } else {
      statements.execute().await(30, TimeUnit.SECONDS);
    }
    assertThat(TestValuesTableFactory.getResults("OrderDetail"))
        .containsExactlyInAnyOrder(
            Row.of(1L, "gold", 105L), Row.of(4L, "silver", 305L), Row.of(6L, "gold", 55L));
    assertThat(TestValuesTableFactory.getResults("TierSummary"))
        .containsExactlyInAnyOrder(Row.of("gold", 160L, 2L), Row.of("silver", 305L, 1L));

    List<RunEvent> starts = new ArrayList<>();
    for (RunEvent event : LineageTestUtils.fromFile(EVENTS.toString())) {
      if (event.getEventType() == EventType.START) {
        starts.add(event);
      }
    }
    assertThat(starts).hasSize(1);
    RunEvent start = starts.get(0);
    assertThat(start.getRun().getRunId()).isNotNull();
    assertThat(start.getInputs())
        .extracting(input -> input.getName())
        .containsExactlyInAnyOrder(identity("RawOrders"), identity("Customers"));
    assertThat(start.getOutputs())
        .extracting(OutputDataset::getName)
        .containsExactlyInAnyOrder(identity("OrderDetail"), identity("TierSummary"));

    String[] rowDependencies = {
      "RawOrders.payload:INDIRECT", "RawOrders.customer_id:INDIRECT",
      "Customers.customer_id:INDIRECT", "Customers.tier:INDIRECT"
    };
    assertField(start, "OrderDetail", "order_id", rowDependencies, "RawOrders.order_id:DIRECT");
    assertField(start, "OrderDetail", "tier", rowDependencies, "Customers.tier:DIRECT");
    assertField(
        start,
        "OrderDetail",
        "net_amount",
        rowDependencies,
        "RawOrders.payload:DIRECT",
        "RawOrders.fee:DIRECT");
    assertField(start, "TierSummary", "tier", rowDependencies, "Customers.tier:DIRECT");
    assertField(
        start,
        "TierSummary",
        "total_amount",
        rowDependencies,
        "RawOrders.payload:DIRECT",
        "RawOrders.fee:DIRECT");
    assertField(start, "TierSummary", "order_count", rowDependencies, "RawOrders.order_id:DIRECT");
    for (OutputDataset output : start.getOutputs()) {
      JsonNode fields = MAPPER.valueToTree(output.getFacets().getColumnLineage().getFields());
      assertThat(fields.size()).isEqualTo(3);
    }
    // Keep successful event evidence per execution path; the next test clears only EVENTS.
    String execution = (restored ? "restored-" : "direct-") + (streaming ? "stream" : "batch");
    Files.copy(
        EVENTS,
        EVENTS.getParent().resolve(execution + ".events.jsonl"),
        StandardCopyOption.REPLACE_EXISTING);
  }

  @Test
  void incompleteLongSessionPlanCannotCreateJob(@InjectMiniCluster MiniCluster cluster)
      throws Exception {
    TableEnvironment environment = environment(true);
    JsonNode plan = MAPPER.readTree(loadSession(environment).compilePlan().asJsonString());
    assertThat(removeLineage(plan)).isEqualTo(2);
    int jobsBefore = cluster.listJobs().get().size();
    assertThatThrownBy(
            () -> environment.loadPlan(PlanReference.fromJsonString(plan.toString())).execute())
        .hasStackTraceContaining("compiled plan does not contain complete column lineage");
    assertThat(cluster.listJobs().get()).hasSize(jobsBefore);
    assertThat(Files.exists(EVENTS)).isFalse();
  }

  private static StatementSet loadSession(TableEnvironment environment) throws Exception {
    String orders =
        TestValuesTableFactory.registerData(
            Arrays.asList(
                Row.of(1L, 10L, "{\"amount\":100,\"status\":\"paid\"}", 5L),
                Row.of(2L, 20L, "{\"amount\":200,\"status\":\"paid\"}", 5L),
                Row.of(3L, 10L, "{\"amount\":900,\"status\":\"cancelled\"}", 5L),
                Row.of(4L, 30L, "{\"amount\":300,\"status\":\"paid\"}", 5L),
                Row.of(5L, 10L, "{\"amount\":null,\"status\":\"paid\"}", 5L),
                Row.of(6L, 10L, "{\"amount\":50,\"status\":\"paid\"}", 5L)));
    String customers =
        TestValuesTableFactory.registerData(
            Arrays.asList(Row.of(10L, "gold"), Row.of(20L, "blocked"), Row.of(30L, "silver")));
    String script;
    try (InputStream stream =
        ColumnLineageLongSessionE2ETest.class.getResourceAsStream(
            "/column-lineage/long-session.sql")) {
      assertThat(stream).isNotNull();
      script =
          new String(stream.readAllBytes(), StandardCharsets.UTF_8)
              .replace("${ordersData}", orders)
              .replace("${customersData}", customers);
    }
    String[] statements = script.split("(?m)^-- statement\\s*$");
    StatementSet result = environment.createStatementSet();
    for (int i = 1; i < statements.length; i++) {
      String sql = statements[i].trim();
      if (sql.startsWith("INSERT INTO")) {
        result.addInsertSql(sql);
      } else {
        environment.executeSql(sql);
      }
    }
    return result;
  }

  private static void assertField(
      RunEvent event, String sink, String field, String[] indirect, String... direct) {
    OutputDataset output =
        event.getOutputs().stream()
            .filter(dataset -> dataset.getName().equals(identity(sink)))
            .findFirst()
            .orElseThrow();
    JsonNode fields = MAPPER.valueToTree(output.getFacets().getColumnLineage().getFields());
    List<String> actual = new ArrayList<>();
    for (JsonNode input : fields.path(field).path("inputFields")) {
      assertThat(input.path("namespace").asText()).isEqualTo("values://FromElementsFunction");
      String name = input.path("name").asText();
      assertThat(name).isIn(identity("RawOrders"), identity("Customers"));
      String table = name.equals(identity("RawOrders")) ? "RawOrders" : "Customers";
      assertThat(input.path("transformations").size()).isEqualTo(1);
      actual.add(
          table
              + "."
              + input.path("field").asText()
              + ":"
              + input.path("transformations").get(0).path("type").asText());
    }
    List<String> expected = new ArrayList<>(Arrays.asList(indirect));
    expected.addAll(Arrays.asList(direct));
    assertThat(actual).as(sink + "." + field).containsExactlyInAnyOrderElementsOf(expected);
  }

  private static int removeLineage(JsonNode node) {
    int removed = node.isObject() && ((ObjectNode) node).remove("columnLineage") != null ? 1 : 0;
    for (JsonNode child : node) {
      removed += removeLineage(child);
    }
    return removed;
  }

  private static String identity(String table) {
    return "`lineage_catalog`.`commerce`.`" + table + "`";
  }

  private static TableEnvironment environment(boolean streaming) {
    TableEnvironment environment =
        TableEnvironment.create(
            (streaming
                    ? EnvironmentSettings.newInstance().inStreamingMode()
                    : EnvironmentSettings.newInstance().inBatchMode())
                .withConfiguration(configuration())
                .build());
    environment.registerCatalog("lineage_catalog", new GenericInMemoryCatalog("lineage_catalog"));
    environment.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
    return environment;
  }

  private static Configuration configuration() {
    Configuration configuration = new Configuration();
    configuration.set(
        JOB_STATUS_CHANGED_LISTENERS,
        Collections.singletonList(OpenLineageJobStatusChangedListenerFactory.class.getName()));
    configuration.setString("openlineage.transport.type", "file");
    configuration.setString("openlineage.transport.location", EVENTS.toString());
    configuration.setString("openlineage.flink.disableCheckpointTracking", "true");
    configuration.setString("parallelism.default", "1");
    return configuration;
  }

  /** Deterministic two-argument UDF: both inputs must remain visible in column lineage. */
  public static class AddFee extends ScalarFunction {
    public Long eval(Long amount, Long fee) {
      return amount == null || fee == null ? null : amount + fee;
    }
  }
}
