/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ColumnLineageRuntimeBoundaryTest {

  private static final String LINEAGE_PACKAGE = "org.apache.flink.streaming.api.lineage.";

  @Test
  void linksOnlyFlinkPublicApiForColumnLineageConversion() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    Class<?> lineageGraph = classLoader.loadClass(LINEAGE_PACKAGE + "LineageGraph");

    Method columnRelations = lineageGraph.getMethod("columnRelations");
    assertThat(columnRelations.getReturnType()).isEqualTo(java.util.List.class);
    assertThat(classLoader.loadClass(LINEAGE_PACKAGE + "ColumnLineageRelation")).isNotNull();
    assertThat(classLoader.loadClass(LINEAGE_PACKAGE + "ColumnLineageInput")).isNotNull();
    assertThat(classLoader.loadClass(LINEAGE_PACKAGE + "ColumnLineageOrigin")).isNotNull();
    assertThat(classLoader.loadClass(LINEAGE_PACKAGE + "ColumnLineageDependencyType")).isNotNull();

    assertThat(classBytes(OpenLineageDatasetExtractor.class))
        .asString(StandardCharsets.ISO_8859_1)
        .doesNotContain("org/apache/flink/table/planner/");
    assertThat(classBytes(LineageGraphConverter.class))
        .asString(StandardCharsets.ISO_8859_1)
        .doesNotContain("org/apache/flink/table/planner/");
  }

  private byte[] classBytes(Class<?> type) throws Exception {
    String resourceName = "/" + type.getName().replace('.', '/') + ".class";
    try (InputStream input = type.getResourceAsStream(resourceName)) {
      assertThat(input).as("class resource %s", resourceName).isNotNull();
      return input.readAllBytes();
    }
  }
}
