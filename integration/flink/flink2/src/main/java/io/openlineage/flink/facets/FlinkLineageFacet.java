/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.facets;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.client.Versions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageGraphObservation;

/** Completeness of the captured metadata, never the success status of the Flink job. */
@Getter
public class FlinkLineageFacet extends OpenLineage.DefaultRunFacet {
  private final String tableStatus;
  private final String columnStatus;
  private final List<String> issues;

  /**
   * Column coverage keyed by native Flink dataset namespace and name, before identifier visitors.
   */
  private final Map<String, Map<String, String>> columnStatuses;

  public FlinkLineageFacet(String tableStatus, String columnStatus, List<String> issues) {
    this(tableStatus, columnStatus, issues, Collections.emptyMap());
  }

  public FlinkLineageFacet(
      String tableStatus,
      String columnStatus,
      List<String> issues,
      Map<String, Map<String, String>> columnStatuses) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.tableStatus = tableStatus;
    this.columnStatus = columnStatus;
    this.issues = List.copyOf(issues);
    Map<String, Map<String, String>> copy = new LinkedHashMap<>();
    columnStatuses.forEach(
        (namespace, statuses) ->
            copy.put(namespace, Collections.unmodifiableMap(new LinkedHashMap<>(statuses))));
    this.columnStatuses = Collections.unmodifiableMap(copy);
  }

  public static FlinkLineageFacet fromStatus(Map<String, String> status) {
    String issueText =
        status.getOrDefault(
            DefaultJobExecutionStatusEvent.LINEAGE_ISSUES,
            "No lineage observation was transferred");
    List<String> issues =
        new ArrayList<>(
            issueText.isEmpty()
                ? Collections.emptyList()
                : java.util.Arrays.asList(issueText.split("\n")));
    String columnStatus =
        status.getOrDefault(DefaultJobExecutionStatusEvent.LINEAGE_COLUMN_STATUS, "UNAVAILABLE");
    Map<String, Map<String, String>> columnStatuses = Collections.emptyMap();
    try {
      columnStatuses =
          OpenLineageClientUtils.newObjectMapper()
              .readValue(
                  status.getOrDefault(DefaultJobExecutionStatusEvent.LINEAGE_COLUMN_STATUSES, "{}"),
                  new TypeReference<Map<String, Map<String, String>>>() {});
      if (columnStatuses == null
          || columnStatuses.values().stream().anyMatch(java.util.Objects::isNull)) {
        throw new IllegalArgumentException("Column status snapshot must contain namespace maps");
      }
    } catch (Exception failure) {
      columnStatuses = Collections.emptyMap();
      columnStatus = "UNAVAILABLE";
      issues.add("Invalid column status snapshot: " + failure.getMessage());
    }
    return new FlinkLineageFacet(
        status.getOrDefault(DefaultJobExecutionStatusEvent.LINEAGE_TABLE_STATUS, "UNAVAILABLE"),
        columnStatus,
        issues,
        columnStatuses);
  }

  public static FlinkLineageFacet fromGraph(LineageGraph graph) {
    if (graph instanceof LineageGraphObservation) {
      LineageGraphObservation observation = (LineageGraphObservation) graph;
      return new FlinkLineageFacet(
          observation.getTableStatus(),
          observation.getColumnStatus(),
          observation.getIssues(),
          observation.getColumnStatuses());
    }
    return new FlinkLineageFacet(
        "UNAVAILABLE",
        "UNAVAILABLE",
        Collections.singletonList("No verified lineage observation status was provided"));
  }
}
