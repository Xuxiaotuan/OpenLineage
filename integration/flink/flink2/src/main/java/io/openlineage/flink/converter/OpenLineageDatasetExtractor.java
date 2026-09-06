/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.converter;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsBuilder;
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.InputFieldTransformations;
import io.openlineage.client.OpenLineage.InputFieldTransformationsBuilder;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceCombinedResolver;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import io.openlineage.flink.visitor.facet.DatasetFacetVisitor;
import io.openlineage.flink.visitor.identifier.DatasetIdentifierVisitor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.lineage.ColumnLineageInput;
import org.apache.flink.streaming.api.lineage.ColumnLineageRelation;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageEdge;
import org.apache.flink.streaming.api.lineage.LineageGraph;

/** Class used to extract datasets from Flink lineage graph. */
class OpenLineageDatasetExtractor {
  private final OpenLineageContext context;
  private final Collection<DatasetFacetVisitor> facetVisitors;
  private final Collection<DatasetIdentifierVisitor> identifierVisitors;
  private final DatasetNamespaceCombinedResolver namespaceResolver;

  OpenLineageDatasetExtractor(OpenLineageContext context, Flink2VisitorFactory visitorFactory) {
    this.context = context;
    this.facetVisitors = visitorFactory.loadDatasetFacetVisitors(context);
    this.identifierVisitors = visitorFactory.loadDatasetIdentifierVisitors(context);
    this.namespaceResolver = new DatasetNamespaceCombinedResolver(context.getConfig());
  }

  Optional<OpenLineage.LineageJobFacet> extractTableLineage(LineageGraph graph) {
    if (graph == null || graph.relations() == null || graph.relations().isEmpty()) {
      // A missing graph is not evidence that every input feeds every output.
      return Optional.empty();
    }
    OpenLineage ol = context.getOpenLineage();
    Map<List<String>, Map<List<String>, OpenLineage.LineageInput>> inputsByOutput =
        new LinkedHashMap<>();
    for (LineageEdge edge : graph.relations()) {
      for (LineageDataset sink : edge.sink().datasets()) {
        Collection<LineageDatasetWithIdentifier> outputs = extractDatasetsWithIdentifiers(sink);
        if (outputs.isEmpty()) {
          throw new IllegalStateException(
              "No OpenLineage identifier for lineage sink " + sink.name());
        }
        for (LineageDatasetWithIdentifier output : outputs) {
          DatasetIdentifier target = output.getDatasetIdentifier();
          Map<List<String>, OpenLineage.LineageInput> inputs =
              inputsByOutput.computeIfAbsent(
                  List.of(target.getNamespace(), target.getName()),
                  ignored -> new LinkedHashMap<>());
          for (LineageDataset source : edge.source().datasets()) {
            Collection<LineageDatasetWithIdentifier> sources =
                extractDatasetsWithIdentifiers(source);
            if (sources.isEmpty()) {
              throw new IllegalStateException(
                  "No OpenLineage identifier for lineage source " + source.name());
            }
            for (LineageDatasetWithIdentifier input : sources) {
              DatasetIdentifier id = input.getDatasetIdentifier();
              inputs.putIfAbsent(
                  List.of(id.getNamespace(), id.getName()),
                  ol.newLineageDatasetInputBuilder()
                      .type(OpenLineage.LineageDatasetInput.Type.DATASET)
                      .namespace(id.getNamespace())
                      .name(id.getName())
                      .build());
            }
          }
        }
      }
    }
    List<OpenLineage.LineageEntry> entries = new ArrayList<>();
    inputsByOutput.forEach(
        (id, inputs) ->
            entries.add(
                ol.newLineageDatasetEntryBuilder()
                    .type(OpenLineage.LineageDatasetEntry.Type.DATASET)
                    .namespace(id.get(0))
                    .name(id.get(1))
                    .inputs(new ArrayList<>(inputs.values()))
                    .build()));
    return Optional.of(ol.newLineageJobFacetBuilder().entries(entries).build());
  }

  List<InputDataset> extractInputs(LineageGraph graph) {
    if (graph == null) {
      return Collections.emptyList();
    }
    return graph.sources().stream()
        .flatMap(source -> source.datasets().stream())
        .flatMap(d -> extractDatasetsWithIdentifiers(d).stream())
        .map(
            d ->
                context
                    .getOpenLineage()
                    .newInputDatasetBuilder()
                    .namespace(d.getDatasetIdentifier().getNamespace())
                    .name(d.getDatasetIdentifier().getName())
                    .facets(convert(d))
                    .build())
        .collect(Collectors.toList());
  }

  List<OutputDataset> extractOutputs(LineageGraph graph) {
    return extractOutputs(graph, Collections.emptyList());
  }

  List<OutputDataset> extractOutputs(
      LineageGraph graph, List<ColumnLineageRelation> columnRelations) {
    if (graph == null) {
      return Collections.emptyList();
    }

    return graph.sinks().stream()
        .flatMap(sink -> sink.datasets().stream())
        .flatMap(d -> extractDatasetsWithIdentifiers(d).stream())
        .map(
            d ->
                context
                    .getOpenLineage()
                    .newOutputDatasetBuilder()
                    .namespace(d.getDatasetIdentifier().getNamespace())
                    .name(d.getDatasetIdentifier().getName())
                    .facets(convert(d, columnRelations))
                    .build())
        .collect(Collectors.toList());
  }

  private OpenLineage.DatasetFacets convert(LineageDatasetWithIdentifier dataset) {
    return convert(dataset, Collections.emptyList());
  }

  private OpenLineage.DatasetFacets convert(
      LineageDatasetWithIdentifier dataset, List<ColumnLineageRelation> columnRelations) {
    DatasetFacetsBuilder facetsBuilder = new DatasetFacetsBuilder();

    if (dataset.getDatasetIdentifier().getSymlinks() != null) {
      facetsBuilder.symlinks(
          context
              .getOpenLineage()
              .newSymlinksDatasetFacet(
                  dataset.getDatasetIdentifier().getSymlinks().stream()
                      .map(
                          i ->
                              context
                                  .getOpenLineage()
                                  .newSymlinksDatasetFacetIdentifiers(
                                      i.getNamespace(), i.getName(), i.getType().toString()))
                      .collect(Collectors.toList())));
    }

    facetVisitors.stream()
        .filter(v -> v.isDefinedAt(dataset))
        .forEach(v -> v.apply(dataset, facetsBuilder));

    buildColumnLineageFacet(dataset.getFlinkDataset(), columnRelations)
        .ifPresent(facetsBuilder::columnLineage);

    return facetsBuilder.build();
  }

  private Optional<ColumnLineageDatasetFacet> buildColumnLineageFacet(
      LineageDataset outputDataset, List<ColumnLineageRelation> columnRelations) {
    List<ColumnLineageRelation> outputRelations =
        columnRelations.stream()
            .filter(relation -> sameDataset(relation.outputDataset(), outputDataset))
            .collect(Collectors.toList());
    if (outputRelations.isEmpty()) {
      return Optional.empty();
    }

    ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder =
        context.getOpenLineage().newColumnLineageDatasetFacetFieldsBuilder();
    for (ColumnLineageRelation relation : outputRelations) {
      fieldsBuilder.put(
          relation.outputField(),
          context
              .getOpenLineage()
              .newColumnLineageDatasetFacetFieldsAdditionalBuilder()
              .inputFields(inputFields(relation))
              // Flink's description applies to the output as a whole, not each dependency edge.
              // This standard field retains that scope, including outputs without input fields.
              .transformationDescription(relation.transformation().orElse(null))
              .build());
    }
    return Optional.of(
        context
            .getOpenLineage()
            .newColumnLineageDatasetFacetBuilder()
            .fields(fieldsBuilder.build())
            .build());
  }

  private List<InputField> inputFields(ColumnLineageRelation relation) {
    List<InputField> inputFields = new ArrayList<>();
    for (ColumnLineageInput input : relation.inputs()) {
      Collection<LineageDatasetWithIdentifier> datasets =
          extractDatasetsWithIdentifiers(input.inputDataset());
      if (datasets.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "Cannot convert column lineage for sink dataset '%s.%s', output field '%s': "
                    + "input dataset '%s.%s', field '%s' has no OpenLineage identifier",
                relation.outputDataset().namespace(),
                relation.outputDataset().name(),
                relation.outputField(),
                input.inputDataset().namespace(),
                input.inputDataset().name(),
                input.inputField()));
      }
      datasets.forEach(dataset -> inputFields.add(inputField(dataset, input)));
    }
    return inputFields;
  }

  private boolean sameDataset(LineageDataset first, LineageDataset second) {
    return Objects.equals(first.namespace(), second.namespace())
        && Objects.equals(first.name(), second.name());
  }

  private InputField inputField(LineageDatasetWithIdentifier dataset, ColumnLineageInput input) {
    InputFieldTransformationsBuilder transformationBuilder =
        new InputFieldTransformationsBuilder().type(input.dependencyType().name());
    InputFieldTransformations transformation = transformationBuilder.build();
    return context
        .getOpenLineage()
        .newInputFieldBuilder()
        .namespace(dataset.getDatasetIdentifier().getNamespace())
        .name(dataset.getDatasetIdentifier().getName())
        .field(input.inputField())
        .transformations(Collections.singletonList(transformation))
        .build();
  }

  private Collection<LineageDatasetWithIdentifier> extractDatasetsWithIdentifiers(
      LineageDataset dataset) {
    List<DatasetIdentifierVisitor> visitors =
        identifierVisitors.stream()
            .filter(v -> v.isDefinedAt(dataset))
            .collect(Collectors.toList());

    if (visitors.isEmpty()) {
      // no visitors to be applied
      return Collections.singletonList(
          new LineageDatasetWithIdentifier(
              namespaceResolver.resolve(new DatasetIdentifier(dataset.name(), dataset.namespace())),
              dataset));
    }

    return identifierVisitors.stream()
        .filter(v -> v.isDefinedAt(dataset))
        .flatMap(v -> v.apply(dataset).stream())
        .map(namespaceResolver::resolve)
        .map(di -> new LineageDatasetWithIdentifier(di, dataset))
        .collect(Collectors.toList());
  }
}
