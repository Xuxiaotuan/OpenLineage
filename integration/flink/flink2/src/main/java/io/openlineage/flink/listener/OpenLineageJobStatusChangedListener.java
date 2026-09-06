/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.converter.LineageGraphConverter;
import io.openlineage.flink.facets.FlinkJobDetailsFacet;
import io.openlineage.flink.facets.FlinkLineageFacet;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.util.JobStatusUtil;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

@Slf4j
public class OpenLineageJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  public static final String FLINK_JOB_FACET_KEY = "flink_job";
  private final OpenLineageContext context;
  private final LineageGraphConverter graphConverter;
  private OpenLineageContinousJobTracker tracker;
  private FlinkLineageFacet lineageStatus;

  public OpenLineageJobStatusChangedListener(Context context, Flink2VisitorFactory visitorFactory) {
    this.context =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    log.info(
        "Creating OpenLineageJobStatusChangedListener with OpenLineageContext: {}", this.context);

    String jobsApiUrl =
        String.format(
            "http://%s:%s/jobs",
            Optional.ofNullable(context.getConfiguration().get(RestOptions.ADDRESS))
                .orElse("localhost"),
            context.getConfiguration().get(RestOptions.PORT));
    tracker =
        new OpenLineageContinousJobTracker(
            Duration.ofSeconds(this.context.getConfig().getTrackingIntervalInSeconds()),
            jobsApiUrl);
    graphConverter = new LineageGraphConverter(this.context, visitorFactory);
  }

  @VisibleForTesting
  OpenLineageJobStatusChangedListener(
      OpenLineageContext context, Flink2VisitorFactory visitorFactory) {
    this.context = context;
    graphConverter = new LineageGraphConverter(this.context, visitorFactory);
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public void onEvent(JobStatusChangedEvent event) {
    context
        .getCircuitBreaker()
        .run(
            () -> {
              if (event instanceof JobCreatedEvent) {
                log.debug("triggered onEvent for JobCreatedEvent: {}", event);
                onJobCreatedEvent((JobCreatedEvent) event);
              } else if (event instanceof DefaultJobExecutionStatusEvent) {
                log.debug("triggered onEvent for DefaultJobExecutionStatusEvent: {}", event);
                onDefaultJobExecutionStatusEvent((DefaultJobExecutionStatusEvent) event);
              } else {
                log.warn("Unsupported event: {}", event.getClass());
              }
              return null;
            });
  }

  private void onJobCreatedEvent(JobCreatedEvent event) {
    lineageStatus = FlinkLineageFacet.fromGraph(event.lineageGraph());
    loadJobId(event);
    RunEvent startEvent;
    try {
      startEvent = graphConverter.convert(event.lineageGraph(), EventType.START);
    } catch (Exception e) {
      log.error("Converting lineage failed; emitting unavailable observation", e);
      List<String> issues = new ArrayList<>(lineageStatus.getIssues());
      issues.add(
          "OpenLineage conversion failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      lineageStatus = new FlinkLineageFacet("UNAVAILABLE", "UNAVAILABLE", issues);
      OpenLineage openLineage = context.getOpenLineage();
      startEvent =
          commonEventBuilder()
              .eventType(EventType.START)
              .run(
                  openLineage
                      .newRunBuilder()
                      .runId(context.getRunUuid())
                      .facets(
                          openLineage
                              .newRunFacetsBuilder()
                              .processing_engine(buildProcessingEngineFacet(openLineage))
                              .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet())
                              .put("flink_lineage", lineageStatus)
                              .build())
                      .build())
              .inputs(Collections.emptyList())
              .outputs(Collections.emptyList())
              .build();
    }
    try {
      context.getEventEmitter().emit(startEvent);
    } catch (Exception e) {
      log.error("Emitting START event failed", e);
    }
    if (context.getConfig().getDisableCheckpointTracking()) {
      log.info("Checkpoint tracking is disabled via disableCheckpointTracking config");
    } else {
      tracker.startTracking(context, this::onJobCheckpoint);
    }
  }

  private void onJobCheckpoint(CheckpointFacet checkpointFacet) {
    log.info("Emitting checkpoint event: {}", checkpointFacet);
    OpenLineage openLineage = context.getOpenLineage();
    RunEvent runEvent =
        commonEventBuilder()
            .eventType(EventType.RUNNING)
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(context.getRunUuid())
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .processing_engine(buildProcessingEngineFacet(openLineage))
                            .put("checkpoints", checkpointFacet)
                            .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet())
                            .put("flink_lineage", lineageStatus)
                            .build())
                    .build())
            .build();

    if (log.isDebugEnabled()) {
      log.debug("Emitting checkpoint event: {}", OpenLineageClientUtils.toJson(runEvent));
    }
    context.getEventEmitter().emit(runEvent);
  }

  private void onDefaultJobExecutionStatusEvent(DefaultJobExecutionStatusEvent event) {
    if (context.getJobId() == null) {
      loadJobId(event);
      java.util.Map<String, String> status = event.getLineageStatus();
      String issues =
          status.getOrDefault(
              DefaultJobExecutionStatusEvent.LINEAGE_ISSUES,
              "No lineage observation was transferred");
      lineageStatus =
          new FlinkLineageFacet(
              status.getOrDefault(
                  DefaultJobExecutionStatusEvent.LINEAGE_TABLE_STATUS, "UNAVAILABLE"),
              status.getOrDefault(
                  DefaultJobExecutionStatusEvent.LINEAGE_COLUMN_STATUS, "UNAVAILABLE"),
              issues.isEmpty()
                  ? java.util.Collections.emptyList()
                  : java.util.Arrays.asList(issues.split("\n")));
    }

    OpenLineage openLineage = context.getOpenLineage();
    RunEvent runEvent =
        commonEventBuilder()
            .eventType(JobStatusUtil.fromJobStatus(event.newStatus()))
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(context.getRunUuid())
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .processing_engine(buildProcessingEngineFacet(openLineage))
                            .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet())
                            .put("flink_lineage", lineageStatus)
                            .build())
                    .build())
            .build();

    context.getEventEmitter().emit(runEvent);
    if (List.of(EventType.COMPLETE, EventType.START).contains(runEvent.getEventType())) {
      tracker.stopTracking();
    }
  }

  private RunEventBuilder commonEventBuilder() {
    return context
        .getOpenLineage()
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .job(
            context
                .getOpenLineage()
                .newJobBuilder()
                .namespace(context.getJobId().getJobNamespace())
                .name(context.getJobId().getJobName())
                .build());
  }

  private OpenLineage.ProcessingEngineRunFacet buildProcessingEngineFacet(OpenLineage openLineage) {
    return openLineage
        .newProcessingEngineRunFacetBuilder()
        .name("flink")
        .version(EnvironmentInformation.getVersion())
        .openlineageAdapterVersion(Versions.getVersion())
        .build();
  }

  private FlinkJobDetailsFacet buildJobDetailsFacet() {
    JobIdentifier jobId = context.getJobId();
    if (jobId == null || jobId.getFlinkJobId() == null) {
      return null;
    }

    String flinkJobId = jobId.getFlinkJobId().toString();
    return new FlinkJobDetailsFacet(flinkJobId);
  }

  private void loadJobId(JobStatusChangedEvent createdEvent) {
    String jobName =
        Optional.ofNullable(context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getName())
            .orElse(createdEvent.jobName());

    String jobNamespace =
        Optional.ofNullable(context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getNamespace())
            .orElse(DEFAULT_NAMESPACE);

    JobIdentifier jobId =
        JobIdentifier.builder()
            .jobName(jobName)
            .jobNamespace(jobNamespace)
            .flinkJobId(createdEvent.jobId())
            .build();
    log.info("JobIdentifier with jobId: {}", jobId.getFlinkJobId());
    context.setJobId(jobId);
    if (createdEvent.jobId() != null) {
      context.setRunUuidFromFlinkJobId(createdEvent.jobId(), java.time.Instant.EPOCH);
    }
  }
}
