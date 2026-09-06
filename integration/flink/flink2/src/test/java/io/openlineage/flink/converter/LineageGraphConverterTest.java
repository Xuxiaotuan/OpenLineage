/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.JobTypeJobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OwnershipJobFacetOwners;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.job.JobConfig;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import io.openlineage.flink.visitor.facet.DatasetFacetVisitor;
import io.openlineage.flink.visitor.identifier.DatasetIdentifierVisitor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.lineage.ColumnLineageDependencyType;
import org.apache.flink.streaming.api.lineage.ColumnLineageInput;
import org.apache.flink.streaming.api.lineage.ColumnLineageOrigin;
import org.apache.flink.streaming.api.lineage.ColumnLineageRelation;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageEdge;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageGraphObservation;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for {@link LineageGraphConverter} */
class LineageGraphConverterTest {

  private static UUID runUuid = UUID.randomUUID();
  Flink2VisitorFactory visitorFactory = mock(Flink2VisitorFactory.class);
  OpenLineageContext context;
  LineageGraphConverter converter;
  FlinkOpenLineageConfig config = mock(FlinkOpenLineageConfig.class);
  JobConfig jobConfig = mock(JobConfig.class);
  LineageGraph graph = mock(LineageGraph.class);

  @BeforeEach
  public void setup() {
    context =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .config(config)
            .build();

    context.setJobId(JobIdentifier.builder().build());
    converter = new LineageGraphConverter(context, visitorFactory);

    when(config.getJobConfig()).thenReturn(jobConfig);
  }

  @Test
  void testJobOwnership() {
    JobConfig.JobOwnersConfig ownersConfig = new JobConfig.JobOwnersConfig();
    ownersConfig.getAdditionalProperties().put("team", "MyTeam");
    ownersConfig.getAdditionalProperties().put("person", "John Smith");

    when(jobConfig.getOwners()).thenReturn(ownersConfig);

    List<OwnershipJobFacetOwners> owners =
        converter.convert(graph, EventType.START).getJob().getFacets().getOwnership().getOwners();
    assertThat(owners).hasSize(2);
    assertThat(owners.stream().filter(o -> "team".equals(o.getType())).findAny().get().getName())
        .isEqualTo("MyTeam");
    assertThat(owners.stream().filter(o -> "person".equals(o.getType())).findAny().get().getName())
        .isEqualTo("John Smith");
  }

  @Test
  void testJobType() {
    SourceLineageVertex source1 =
        sourceVertexOf(Boundedness.CONTINUOUS_UNBOUNDED, Collections.emptyList());
    SourceLineageVertex source2 = sourceVertexOf(Boundedness.BOUNDED, Collections.emptyList());

    when(graph.sources()).thenReturn(Arrays.asList(source1, source2));

    JobTypeJobFacet jobTypeFacet =
        converter.convert(graph, EventType.START).getJob().getFacets().getJobType();
    assertThat(jobTypeFacet.getIntegration()).isEqualTo("FLINK");
    assertThat(jobTypeFacet.getJobType()).isEqualTo("JOB");
    assertThat(jobTypeFacet.getProcessingType()).isEqualTo("STREAMING");

    when(graph.sources()).thenReturn(Arrays.asList(source2, source2));
    assertThat(
            converter
                .convert(graph, EventType.START)
                .getJob()
                .getFacets()
                .getJobType()
                .getProcessingType())
        .isEqualTo("BATCH");
  }

  @Test
  void testFacetVisitors() {
    DatasetFacet openlineageSourceFacet = mock(DatasetFacet.class);
    LineageDatasetWithIdentifier sourceDataset = lineageDatasetOf("sourceName", "sourceNamespace");
    SourceLineageVertex source =
        sourceVertexOf(
            Boundedness.CONTINUOUS_UNBOUNDED,
            Collections.singletonList(sourceDataset.getFlinkDataset()));

    DatasetFacet openlineageSinkFacet = mock(DatasetFacet.class);
    LineageDatasetWithIdentifier sinkDataset = lineageDatasetOf("sinkName", "sinkNamespace");
    LineageVertex sink =
        new LineageVertex() {
          @Override
          public List<LineageDataset> datasets() {
            return List.of(sinkDataset.getFlinkDataset());
          }
        };

    when(graph.sources()).thenReturn(List.of(source));
    when(graph.sinks()).thenReturn(List.of(sink));

    when(visitorFactory.loadDatasetFacetVisitors(context))
        .thenReturn(
            List.of(
                new TestingDatasetFacetVisitor(
                    Map.of(
                        sourceDataset,
                        openlineageSourceFacet,
                        sinkDataset,
                        openlineageSinkFacet))));
    when(visitorFactory.loadDatasetIdentifierVisitors(context)).thenReturn(Collections.emptyList());
    LineageGraphConverter converter = new LineageGraphConverter(context, visitorFactory);

    List<InputDataset> inputs = converter.convert(graph, EventType.START).getInputs();
    assertThat(inputs).hasSize(1);
    assertThat(inputs.get(0))
        .hasFieldOrPropertyWithValue("name", "sourceName")
        .hasFieldOrPropertyWithValue("namespace", "sourceNamespace");
    assertThat(inputs.get(0).getFacets().getAdditionalProperties().get("facet"))
        .isEqualTo(openlineageSourceFacet);

    List<OutputDataset> outputs = converter.convert(graph, EventType.START).getOutputs();
    assertThat(outputs).hasSize(1);
    assertThat(outputs.get(0))
        .hasFieldOrPropertyWithValue("name", "sinkName")
        .hasFieldOrPropertyWithValue("namespace", "sinkNamespace");
    assertThat(outputs.get(0).getFacets().getAdditionalProperties().get("facet"))
        .isEqualTo(openlineageSinkFacet);
  }

  @Test
  void testDatasetIdentifierFacetVisitors() {
    LineageDatasetWithIdentifier dataset = lineageDatasetOf("datasetName", "namespace");
    SourceLineageVertex source =
        sourceVertexOf(
            Boundedness.CONTINUOUS_UNBOUNDED, Collections.singletonList(dataset.getFlinkDataset()));

    when(graph.sources()).thenReturn(List.of(source));
    when(graph.sinks()).thenReturn(Collections.emptyList());

    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(List.of(new TestingDatasetIdentifierVisitor()));

    LineageGraphConverter converter = new LineageGraphConverter(context, visitorFactory);
    List<InputDataset> inputs = converter.convert(graph, EventType.START).getInputs();
    assertThat(inputs).hasSize(2);
    assertThat(
            inputs.stream()
                .map(id -> new DatasetIdentifier(id.getName(), id.getNamespace()))
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            new DatasetIdentifier("datasetName1", "namespace"),
            new DatasetIdentifier("datasetName2", "namespace"));

    assertThat(inputs.get(0).getFacets().getSymlinks().getIdentifiers().get(0))
        .hasFieldOrPropertyWithValue("namespace", "namespace")
        .hasFieldOrPropertyWithValue("name", "table1")
        .hasFieldOrPropertyWithValue("type", "TABLE");
  }

  @Test
  void keepsOutputTransformationOffIndividualInputEdges() throws Exception {
    LineageDataset source = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset sink = lineageDatasetOf("totals", "warehouse").getFlinkDataset();
    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(source))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(sink)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    sink,
                    "total",
                    List.of(
                        inputOf(source, "amount", ColumnLineageDependencyType.DIRECT),
                        inputOf(source, "region", ColumnLineageDependencyType.INDIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    "AGGREGATION,GROUP_BY")));
    JsonNode field =
        outputsJson().get(0).path("facets").path("columnLineage").path("fields").path("total");
    assertThat(field.path("transformationDescription").asText()).isEqualTo("AGGREGATION,GROUP_BY");
    for (JsonNode input : field.path("inputFields")) {
      assertThat(input.path("transformations").get(0).has("description")).isFalse();
    }
  }

  @Test
  void convertsDirectAndIndirectMultipleInputFieldsIntoColumnLineageJson() throws Exception {
    LineageDataset orders = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset customers = lineageDatasetOf("customers", "warehouse").getFlinkDataset();
    LineageDataset totals = lineageDatasetOf("daily_totals", "warehouse").getFlinkDataset();

    when(graph.sources())
        .thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(orders, customers))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(totals)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    totals,
                    "total",
                    List.of(
                        inputOf(orders, "amount", ColumnLineageDependencyType.DIRECT),
                        inputOf(customers, "tier", ColumnLineageDependencyType.INDIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    "amount * tier_multiplier")));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"daily_totals\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"total\":{\"transformationDescription\":\"amount * tier_multiplier\",\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"amount\","
                        + "\"transformations\":[{\"type\":\"DIRECT\"}]},"
                        + "{\"namespace\":\"warehouse\",\"name\":\"customers\",\"field\":\"tier\","
                        + "\"transformations\":[{\"type\":\"INDIRECT\"}]}]}}}}}]"));
  }

  @Test
  void convertsConstantOutputFieldIntoAnEmptyColumnLineageInputList() throws Exception {
    LineageDataset constants = lineageDatasetOf("constants", "warehouse").getFlinkDataset();

    when(graph.sources()).thenReturn(Collections.emptyList());
    when(graph.sinks()).thenReturn(List.of(vertexOf(constants)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    constants,
                    "source_system",
                    Collections.emptyList(),
                    ColumnLineageOrigin.CONSTANT,
                    null)));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"constants\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"source_system\":{\"inputFields\":[]}}}}}]"));
  }

  @Test
  void convertsConstantOutputFieldIndirectFilterDependencyIntoColumnLineageJson() throws Exception {
    LineageDataset orders = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset constants = lineageDatasetOf("constants", "warehouse").getFlinkDataset();

    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(orders))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(constants)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    constants,
                    "source_system",
                    List.of(inputOf(orders, "is_current", ColumnLineageDependencyType.INDIRECT)),
                    ColumnLineageOrigin.CONSTANT,
                    "WHERE is_current")));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"constants\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"source_system\":{\"transformationDescription\":\"WHERE is_current\",\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"is_current\","
                        + "\"transformations\":[{\"type\":\"INDIRECT\"}]}]}}}}}]"));
  }

  @Test
  void createsSeparateColumnLineageFacetsForMultipleSinks() throws Exception {
    LineageDataset source = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset daily = lineageDatasetOf("daily_orders", "warehouse").getFlinkDataset();
    LineageDataset monthly = lineageDatasetOf("monthly_orders", "warehouse").getFlinkDataset();

    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(source))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(daily), vertexOf(monthly)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    daily,
                    "order_id",
                    List.of(inputOf(source, "id", ColumnLineageDependencyType.DIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    null),
                relationOf(
                    monthly,
                    "order_count",
                    List.of(inputOf(source, "id", ColumnLineageDependencyType.INDIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    "COUNT(id)")));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"daily_orders\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"order_id\":{\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"id\","
                        + "\"transformations\":[{\"type\":\"DIRECT\"}]}]}}}}},"
                        + "{\"namespace\":\"warehouse\",\"name\":\"monthly_orders\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"order_count\":{\"transformationDescription\":\"COUNT(id)\",\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"id\","
                        + "\"transformations\":[{\"type\":\"INDIRECT\"}]}]}}}}}]"));
  }

  @Test
  void matchesColumnRelationToSinkByStableDatasetIdentity() throws Exception {
    LineageDataset source = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset sinkDataset = lineageDatasetOf("daily_orders", "warehouse").getFlinkDataset();
    LineageDataset relationDataset =
        lineageDatasetOf("daily_orders", "warehouse").getFlinkDataset();

    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(source))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(sinkDataset)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    relationDataset,
                    "order_id",
                    List.of(inputOf(source, "id", ColumnLineageDependencyType.DIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    null)));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"daily_orders\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"order_id\":{\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"id\","
                        + "\"transformations\":[{\"type\":\"DIRECT\"}]}]}}}}}]"));
  }

  @Test
  void convertsNullGraphWithoutColumnLineageLookup() {
    assertThat(converter.convert(null, EventType.START).getInputs()).isEmpty();
    assertThat(converter.convert(null, EventType.START).getOutputs()).isEmpty();
  }

  @Test
  void rejectsInputFieldWithoutAnOpenLineageDatasetIdentifier() {
    LineageDataset source = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset sink = lineageDatasetOf("daily_orders", "warehouse").getFlinkDataset();

    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(List.of(new EmptyDatasetIdentifierVisitor(source)));
    converter = new LineageGraphConverter(context, visitorFactory);
    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(source))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(sink)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    sink,
                    "order_id",
                    List.of(inputOf(source, "id", ColumnLineageDependencyType.DIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    null)));

    assertThatThrownBy(() -> converter.convert(graph, EventType.START))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("daily_orders")
        .hasMessageContaining("order_id")
        .hasMessageContaining("orders")
        .hasMessageContaining("id");
  }

  @Test
  void preservesMultipleOpenLineageIdentifiersForOneInputField() throws Exception {
    LineageDataset source = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset sink = lineageDatasetOf("daily_orders", "warehouse").getFlinkDataset();

    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(
            List.of(
                new MultipleDatasetIdentifierVisitor(
                    source,
                    List.of(
                        new DatasetIdentifier("orders_primary", "warehouse"),
                        new DatasetIdentifier("orders_replica", "warehouse")))));
    converter = new LineageGraphConverter(context, visitorFactory);
    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(source))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(sink)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    sink,
                    "order_id",
                    List.of(inputOf(source, "id", ColumnLineageDependencyType.DIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    null)));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"daily_orders\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"order_id\":{\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders_primary\",\"field\":\"id\","
                        + "\"transformations\":[{\"type\":\"DIRECT\"}]},"
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders_replica\",\"field\":\"id\","
                        + "\"transformations\":[{\"type\":\"DIRECT\"}]}]}}}}}]"));
  }

  @Test
  void convertsSystemOutputFieldIntoAnEmptyColumnLineageInputList() throws Exception {
    LineageDataset systemOutput = lineageDatasetOf("audit", "warehouse").getFlinkDataset();

    when(graph.sources()).thenReturn(Collections.emptyList());
    when(graph.sinks()).thenReturn(List.of(vertexOf(systemOutput)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    systemOutput,
                    "processed_at",
                    Collections.emptyList(),
                    ColumnLineageOrigin.SYSTEM,
                    "CURRENT_TIMESTAMP")));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"audit\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"processed_at\":{\"transformationDescription\":\"CURRENT_TIMESTAMP\",\"inputFields\":[]}}}}}]"));
  }

  @Test
  void convertsSystemOutputFieldIndirectGroupAndFilterDependenciesIntoColumnLineageJson()
      throws Exception {
    LineageDataset orders = lineageDatasetOf("orders", "warehouse").getFlinkDataset();
    LineageDataset systemOutput = lineageDatasetOf("audit", "warehouse").getFlinkDataset();

    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(orders))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(systemOutput)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    systemOutput,
                    "processed_at",
                    List.of(
                        inputOf(orders, "region", ColumnLineageDependencyType.INDIRECT),
                        inputOf(orders, "is_current", ColumnLineageDependencyType.INDIRECT)),
                    ColumnLineageOrigin.SYSTEM,
                    "GROUP BY region; FILTER is_current")));

    assertThat(outputsJson())
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"warehouse\",\"name\":\"audit\",\"facets\":{"
                        + "\"columnLineage\":{\"fields\":{\"processed_at\":{\"transformationDescription\":\"GROUP BY region; FILTER is_current\",\"inputFields\":["
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"region\","
                        + "\"transformations\":[{\"type\":\"INDIRECT\"}]},"
                        + "{\"namespace\":\"warehouse\",\"name\":\"orders\",\"field\":\"is_current\","
                        + "\"transformations\":[{\"type\":\"INDIRECT\"}]}]}}}}}]"));
  }

  @Test
  void independentSinksKeepExactTablePairs() throws Exception {
    LineageDataset a = lineageDatasetOf("A", "ns").getFlinkDataset();
    LineageDataset b = lineageDatasetOf("B", "ns").getFlinkDataset();
    LineageVertex x = vertexOf(lineageDatasetOf("X", "ns").getFlinkDataset());
    LineageVertex y = vertexOf(lineageDatasetOf("Y", "ns").getFlinkDataset());
    SourceLineageVertex av = sourceVertexOf(Boundedness.BOUNDED, List.of(a));
    SourceLineageVertex bv = sourceVertexOf(Boundedness.BOUNDED, List.of(b));
    when(graph.sources()).thenReturn(List.of(av, bv));
    when(graph.sinks()).thenReturn(List.of(x, y));
    when(graph.relations()).thenReturn(List.of(edgeOf(av, x), edgeOf(bv, y), edgeOf(av, x)));
    JsonNode json =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(
                OpenLineageClientUtils.newObjectMapper()
                    .writeValueAsString(converter.convert(graph, EventType.START)));
    assertThat(json.path("job").path("facets").path("lineage").path("entries"))
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"X\",\"inputs\":[{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"A\"}]},"
                        + "{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"Y\",\"inputs\":[{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"B\"}]}]"));
  }

  @Test
  void sameFieldKeepsDirectAndIndirectRolesInSerializedEvent() throws Exception {
    LineageDataset source = lineageDatasetOf("T", "ns").getFlinkDataset();
    LineageDataset sink = lineageDatasetOf("S", "ns").getFlinkDataset();
    when(graph.sources()).thenReturn(List.of(sourceVertexOf(Boundedness.BOUNDED, List.of(source))));
    when(graph.sinks()).thenReturn(List.of(vertexOf(sink)));
    when(graph.columnRelations())
        .thenReturn(
            List.of(
                relationOf(
                    sink,
                    "a",
                    List.of(
                        inputOf(source, "a", ColumnLineageDependencyType.DIRECT),
                        inputOf(source, "a", ColumnLineageDependencyType.INDIRECT)),
                    ColumnLineageOrigin.INPUT_FIELDS,
                    "FILTER")));
    assertThat(
            outputsJson()
                .get(0)
                .path("facets")
                .path("columnLineage")
                .path("fields")
                .path("a")
                .path("inputFields"))
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"namespace\":\"ns\",\"name\":\"T\",\"field\":\"a\",\"transformations\":[{\"type\":\"DIRECT\"},{\"type\":\"INDIRECT\"}]}]"));
  }

  @Test
  void incompleteTableObservationOmitsPrecisePairsButKeepsKnownDatasets() {
    SourceLineageVertex source =
        sourceVertexOf(Boundedness.BOUNDED, List.of(lineageDatasetOf("A", "ns").getFlinkDataset()));
    LineageVertex sink = vertexOf(lineageDatasetOf("X", "ns").getFlinkDataset());
    when(graph.sources()).thenReturn(List.of(source));
    when(graph.sinks()).thenReturn(List.of(sink));
    when(graph.relations()).thenReturn(List.of(edgeOf(source, sink)));
    for (String tableStatus : List.of("PARTIAL", "UNAVAILABLE")) {
      OpenLineage.RunEvent event =
          converter.convert(
              new LineageGraphObservation(
                  graph, tableStatus, "UNAVAILABLE", List.of("missing metadata")),
              EventType.START);
      assertThat(event.getJob().getFacets().getLineage()).isNull();
      assertThat(event.getInputs()).extracting(InputDataset::getName).containsExactly("A");
      assertThat(event.getOutputs()).extracting(OutputDataset::getName).containsExactly("X");
    }
    assertThat(
            converter
                .convert(
                    new LineageGraphObservation(graph, "COMPLETE", "UNAVAILABLE", List.of()),
                    EventType.START)
                .getJob()
                .getFacets()
                .getLineage())
        .isNotNull();
  }

  @Test
  void sourceLessSinkHasExplicitEmptyTableInputs() throws Exception {
    when(graph.sinks())
        .thenReturn(List.of(vertexOf(lineageDatasetOf("constants", "ns").getFlinkDataset())));
    JsonNode entries =
        OpenLineageClientUtils.newObjectMapper()
            .valueToTree(converter.convert(graph, EventType.START).getJob().getFacets())
            .path("lineage")
            .path("entries");
    assertThat(entries)
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"constants\",\"inputs\":[]}]"));
  }

  @Test
  void sourceLessSinkIsRetainedAlongsideRelatedSink() throws Exception {
    SourceLineageVertex source =
        sourceVertexOf(Boundedness.BOUNDED, List.of(lineageDatasetOf("A", "ns").getFlinkDataset()));
    LineageVertex sink = vertexOf(lineageDatasetOf("X", "ns").getFlinkDataset());
    LineageVertex constants = vertexOf(lineageDatasetOf("constants", "ns").getFlinkDataset());
    when(graph.sources()).thenReturn(List.of(source));
    when(graph.sinks()).thenReturn(List.of(sink, constants));
    when(graph.relations()).thenReturn(List.of(edgeOf(source, sink)));
    JsonNode entries =
        OpenLineageClientUtils.newObjectMapper()
            .valueToTree(converter.convert(graph, EventType.START).getJob().getFacets())
            .path("lineage")
            .path("entries");
    assertThat(entries)
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"X\",\"inputs\":[{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"A\"}]},"
                        + "{\"type\":\"DATASET\",\"namespace\":\"ns\",\"name\":\"constants\",\"inputs\":[]}]"));
  }

  @Test
  void tablePairsUseResolvedDatasetIdentifiersAndMergeInputs() throws Exception {
    LineageDataset source = lineageDatasetOf("logical", "catalog").getFlinkDataset();
    LineageDataset sink = lineageDatasetOf("sink", "catalog").getFlinkDataset();
    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(
            List.of(
                new MultipleDatasetIdentifierVisitor(
                    source,
                    List.of(
                        new DatasetIdentifier("orders", "physical-one"),
                        new DatasetIdentifier("orders", "physical-two"))),
                new MultipleDatasetIdentifierVisitor(
                    sink, List.of(new DatasetIdentifier("result", "warehouse")))));
    converter = new LineageGraphConverter(context, visitorFactory);
    SourceLineageVertex av = sourceVertexOf(Boundedness.BOUNDED, List.of(source));
    SourceLineageVertex bv =
        sourceVertexOf(
            Boundedness.BOUNDED, List.of(lineageDatasetOf("other", "catalog").getFlinkDataset()));
    LineageVertex sv = vertexOf(sink);
    when(graph.sources()).thenReturn(List.of(av, bv));
    when(graph.sinks()).thenReturn(List.of(sv));
    when(graph.relations()).thenReturn(List.of(edgeOf(av, sv), edgeOf(bv, sv)));
    JsonNode entries =
        OpenLineageClientUtils.newObjectMapper()
            .valueToTree(converter.convert(graph, EventType.START).getJob().getFacets())
            .path("lineage")
            .path("entries");
    assertThat(entries)
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree(
                    "[{\"type\":\"DATASET\",\"namespace\":\"warehouse\",\"name\":\"result\",\"inputs\":["
                        + "{\"type\":\"DATASET\",\"namespace\":\"physical-one\",\"name\":\"orders\"},"
                        + "{\"type\":\"DATASET\",\"namespace\":\"physical-two\",\"name\":\"orders\"},"
                        + "{\"type\":\"DATASET\",\"namespace\":\"catalog\",\"name\":\"other\"}]}]"));
  }

  @Test
  void rejectsUnresolvableTableEdgeRatherThanDroppingIt() {
    LineageDataset source = lineageDatasetOf("unresolved", "ns").getFlinkDataset();
    LineageDataset sink = lineageDatasetOf("sink", "ns").getFlinkDataset();
    when(visitorFactory.loadDatasetIdentifierVisitors(context))
        .thenReturn(List.of(new EmptyDatasetIdentifierVisitor(source)));
    converter = new LineageGraphConverter(context, visitorFactory);
    when(graph.relations())
        .thenReturn(
            List.of(edgeOf(sourceVertexOf(Boundedness.BOUNDED, List.of(source)), vertexOf(sink))));
    assertThatThrownBy(() -> converter.convert(graph, EventType.START))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unresolved");
  }

  private LineageEdge edgeOf(SourceLineageVertex source, LineageVertex sink) {
    return new LineageEdge() {
      @Override
      public SourceLineageVertex source() {
        return source;
      }

      @Override
      public LineageVertex sink() {
        return sink;
      }
    };
  }

  private JsonNode outputsJson() throws Exception {
    JsonNode outputs =
        OpenLineageClientUtils.newObjectMapper()
            .readTree(
                OpenLineageClientUtils.newObjectMapper()
                    .writeValueAsString(converter.convert(graph, EventType.START).getOutputs()));
    for (JsonNode output : outputs) {
      ObjectNode facets = (ObjectNode) output.get("facets");
      ObjectNode columnLineage = (ObjectNode) facets.get("columnLineage");
      columnLineage.remove(List.of("_producer", "_schemaURL"));
      JsonNode symlinks = facets.get("symlinks");
      if (symlinks != null
          && symlinks.path("identifiers").isArray()
          && symlinks.path("identifiers").isEmpty()) {
        facets.remove("symlinks");
      }
    }
    return outputs;
  }

  private ColumnLineageRelation relationOf(
      LineageDataset outputDataset,
      String outputField,
      List<ColumnLineageInput> inputs,
      ColumnLineageOrigin origin,
      String transformation) {
    return new ColumnLineageRelation() {
      @Override
      public LineageDataset outputDataset() {
        return outputDataset;
      }

      @Override
      public String outputField() {
        return outputField;
      }

      @Override
      public List<ColumnLineageInput> inputs() {
        return inputs;
      }

      @Override
      public ColumnLineageOrigin origin() {
        return origin;
      }

      @Override
      public Optional<String> transformation() {
        return Optional.ofNullable(transformation);
      }
    };
  }

  private ColumnLineageInput inputOf(
      LineageDataset dataset, String field, ColumnLineageDependencyType dependencyType) {
    return new ColumnLineageInput() {
      @Override
      public LineageDataset inputDataset() {
        return dataset;
      }

      @Override
      public String inputField() {
        return field;
      }

      @Override
      public ColumnLineageDependencyType dependencyType() {
        return dependencyType;
      }
    };
  }

  private LineageDatasetWithIdentifier lineageDatasetOf(String name, String namespace) {
    return new LineageDatasetWithIdentifier(
        new DatasetIdentifier(name, namespace),
        new LineageDataset() {
          @Override
          public String name() {
            return name;
          }

          @Override
          public String namespace() {
            return namespace;
          }

          @Override
          public Map<String, LineageDatasetFacet> facets() {
            return Collections.emptyMap();
          }
        });
  }

  private SourceLineageVertex sourceVertexOf(
      Boundedness boundedness, List<LineageDataset> datasets) {
    return new SourceLineageVertex() {
      @Override
      public Boundedness boundedness() {
        return boundedness;
      }

      @Override
      public List<LineageDataset> datasets() {
        return datasets;
      }
    };
  }

  private LineageVertex vertexOf(LineageDataset dataset) {
    return new LineageVertex() {
      @Override
      public List<LineageDataset> datasets() {
        return List.of(dataset);
      }
    };
  }

  private static class TestingDatasetFacetVisitor implements DatasetFacetVisitor {

    private final Map<LineageDatasetWithIdentifier, DatasetFacet> behaviour;

    private TestingDatasetFacetVisitor(Map<LineageDatasetWithIdentifier, DatasetFacet> behaviour) {
      this.behaviour = behaviour;
    }

    @Override
    public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
      return behaviour.containsKey(dataset);
    }

    @Override
    public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
      builder.put("facet", behaviour.get(dataset));
    }
  }

  private static class TestingDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
    @Override
    public boolean isDefinedAt(LineageDataset dataset) {
      return true;
    }

    @Override
    public Collection<DatasetIdentifier> apply(LineageDataset dataset) {
      return Arrays.asList(
          new DatasetIdentifier(
              "datasetName1",
              "namespace",
              Arrays.asList(new Symlink("table1", "namespace", SymlinkType.TABLE))),
          new DatasetIdentifier("datasetName2", "namespace"));
    }
  }

  private static class EmptyDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
    private final LineageDataset dataset;

    private EmptyDatasetIdentifierVisitor(LineageDataset dataset) {
      this.dataset = dataset;
    }

    @Override
    public boolean isDefinedAt(LineageDataset candidate) {
      return dataset == candidate;
    }

    @Override
    public Collection<DatasetIdentifier> apply(LineageDataset candidate) {
      return Collections.emptyList();
    }
  }

  private static class MultipleDatasetIdentifierVisitor implements DatasetIdentifierVisitor {
    private final LineageDataset dataset;
    private final List<DatasetIdentifier> identifiers;

    private MultipleDatasetIdentifierVisitor(
        LineageDataset dataset, List<DatasetIdentifier> identifiers) {
      this.dataset = dataset;
      this.identifiers = identifiers;
    }

    @Override
    public boolean isDefinedAt(LineageDataset candidate) {
      return dataset == candidate;
    }

    @Override
    public Collection<DatasetIdentifier> apply(LineageDataset candidate) {
      return identifiers;
    }
  }
}
