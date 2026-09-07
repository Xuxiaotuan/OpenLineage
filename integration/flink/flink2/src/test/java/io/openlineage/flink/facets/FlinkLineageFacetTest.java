/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.facets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.junit.jupiter.api.Test;

class FlinkLineageFacetTest {
  @Test
  void terminalSnapshotRetainsPerOutputTableCoverage() {
    FlinkLineageFacet facet =
        FlinkLineageFacet.fromStatus(
            Map.of(
                "internal.lineage.table-status",
                "PARTIAL",
                "internal.lineage.table-statuses",
                "{\"ns\":{\"good\":\"COMPLETE\",\"shared\":\"UNAVAILABLE\"}}"));
    assertThat(
            io.openlineage.client.OpenLineageClientUtils.newObjectMapper()
                .valueToTree(facet)
                .path("tableStatuses")
                .path("ns")
                .path("good")
                .asText())
        .isEqualTo("COMPLETE");
  }

  @Test
  void columnStatusSnapshotCannotBeChangedThroughInputOrGetter() {
    Map<String, String> sinks = new LinkedHashMap<>(Map.of("sink", "COMPLETE"));
    Map<String, Map<String, String>> input = new LinkedHashMap<>(Map.of("ns", sinks));
    FlinkLineageFacet facet = new FlinkLineageFacet("COMPLETE", "COMPLETE", List.of(), input);
    sinks.put("sink", "UNAVAILABLE");
    input.clear();
    assertThat(facet.getColumnStatuses()).isEqualTo(Map.of("ns", Map.of("sink", "COMPLETE")));
    assertThatThrownBy(() -> facet.getColumnStatuses().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> facet.getColumnStatuses().get("ns").put("sink", "UNAVAILABLE"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void malformedColumnSnapshotDoesNotSuppressLifecycleStatus() {
    FlinkLineageFacet facet =
        FlinkLineageFacet.fromStatus(
            Map.of(
                DefaultJobExecutionStatusEvent.LINEAGE_TABLE_STATUS,
                "COMPLETE",
                DefaultJobExecutionStatusEvent.LINEAGE_COLUMN_STATUS,
                "PARTIAL",
                "internal.lineage.column-statuses",
                "not-json"));
    assertThat(facet.getTableStatus()).isEqualTo("COMPLETE");
    assertThat(facet.getColumnStatus()).isEqualTo("UNAVAILABLE");
    assertThat(facet.getColumnStatuses()).isEmpty();
    assertThat(facet.getIssues())
        .anyMatch(issue -> issue.contains("Invalid column status snapshot"));
  }
}
