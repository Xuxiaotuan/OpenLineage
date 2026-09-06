/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.facets;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.client.Versions;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageGraphObservation;

/** Completeness of the captured metadata, never the success status of the Flink job. */
@Getter
public class FlinkLineageFacet extends OpenLineage.DefaultRunFacet {
  private final String tableStatus;
  private final String columnStatus;
  private final List<String> issues;

  public FlinkLineageFacet(String tableStatus, String columnStatus, List<String> issues) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.tableStatus = tableStatus;
    this.columnStatus = columnStatus;
    this.issues = List.copyOf(issues);
  }

  public static FlinkLineageFacet fromGraph(LineageGraph graph) {
    if (graph instanceof LineageGraphObservation) {
      LineageGraphObservation observation = (LineageGraphObservation) graph;
      return new FlinkLineageFacet(
          observation.getTableStatus(), observation.getColumnStatus(), observation.getIssues());
    }
    return new FlinkLineageFacet(
        "UNAVAILABLE",
        "UNAVAILABLE",
        Collections.singletonList("No verified lineage observation status was provided"));
  }
}
