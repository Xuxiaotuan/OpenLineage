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
import io.openlineage.client.utils.UUIDUtils;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.core.execution.SubmissionIdentity;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

@Slf4j
public class OpenLineageJobStatusChangedListener implements JobStatusChangedListener {
  public static final String DEFAULT_NAMESPACE = "flink-jobs";
  public static final String FLINK_JOB_FACET_KEY = "flink_job";
  // One bounded sender per adapter classloader. Idle daemon threads do not retain a client JVM.
  // This is best effort: JVM exit and a full queue can lose events.
  private static final java.util.concurrent.Executor DELIVERY =
      new java.util.concurrent.ThreadPoolExecutor(
          0,
          1,
          30,
          java.util.concurrent.TimeUnit.SECONDS,
          new java.util.concurrent.ArrayBlockingQueue<>(1024),
          task -> {
            Thread thread = new Thread(task, "openlineage-flink-delivery");
            thread.setDaemon(true);
            return thread;
          });
  private final java.util.concurrent.Executor delivery;
  private final OpenLineageContext context;
  private final Flink2VisitorFactory visitorFactory;
  private final String jobsApiUrl;
  private final java.util.Map<SubmissionKey, JobState> jobs =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final java.util.Set<SubmissionKey> completedJobs =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  public OpenLineageJobStatusChangedListener(Context context, Flink2VisitorFactory visitorFactory) {
    this.delivery = DELIVERY;
    this.context =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build();
    log.info(
        "Creating OpenLineageJobStatusChangedListener with OpenLineageContext: {}", this.context);

    this.visitorFactory = visitorFactory;
    jobsApiUrl =
        String.format(
            "http://%s:%s/jobs",
            Optional.ofNullable(context.getConfiguration().get(RestOptions.ADDRESS))
                .orElse("localhost"),
            context.getConfiguration().get(RestOptions.PORT));
  }

  @VisibleForTesting
  OpenLineageJobStatusChangedListener(
      OpenLineageContext context, Flink2VisitorFactory visitorFactory) {
    this(context, visitorFactory, DELIVERY);
  }

  @VisibleForTesting
  OpenLineageJobStatusChangedListener(
      OpenLineageContext context,
      Flink2VisitorFactory visitorFactory,
      java.util.concurrent.Executor delivery) {
    this.delivery = delivery;
    this.context = context;
    this.visitorFactory = visitorFactory;
    this.jobsApiUrl = "http://localhost:8081/jobs";
  }

  @Override
  @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public synchronized void onEvent(JobStatusChangedEvent event) {
    SubmissionKey key = SubmissionKey.from(event);
    boolean terminal =
        event instanceof DefaultJobExecutionStatusEvent
            && List.of(EventType.COMPLETE, EventType.FAIL, EventType.ABORT)
                .contains(
                    JobStatusUtil.fromJobStatus(
                        ((DefaultJobExecutionStatusEvent) event).newStatus()));
    JobState terminalState = null;
    if (terminal) {
      if (!completedJobs.add(key)) {
        return;
      }
      terminalState = jobs.remove(key);
      if (terminalState == null) {
        terminalState = new JobState(key);
      }
      terminalState.stopTracking();
    }
    JobState capturedTerminalState = terminalState;
    context
        .getCircuitBreaker()
        .run(
            () -> {
              if (!(event instanceof JobCreatedEvent)
                  && !(event instanceof DefaultJobExecutionStatusEvent)) {
                log.warn("Unsupported event: {}", event.getClass());
                return null;
              }
              if (!terminal && completedJobs.contains(key)) {
                return null;
              }
              JobState state =
                  terminal ? capturedTerminalState : jobs.computeIfAbsent(key, JobState::new);
              synchronized (state) {
                if (!terminal && completedJobs.contains(key)) {
                  jobs.remove(key, state);
                  state.stopTracking();
                  return null;
                }
                if (event instanceof JobCreatedEvent) {
                  log.debug("triggered onEvent for JobCreatedEvent: {}", event);
                  onJobCreatedEvent(state, (JobCreatedEvent) event);
                } else if (event instanceof DefaultJobExecutionStatusEvent) {
                  log.debug("triggered onEvent for DefaultJobExecutionStatusEvent: {}", event);
                  onDefaultJobExecutionStatusEvent(state, (DefaultJobExecutionStatusEvent) event);
                } else {
                  log.warn("Unsupported event: {}", event.getClass());
                }
              }
              return null;
            });
  }

  @Value
  private static class SubmissionKey {
    JobID jobId;
    String submissionId;

    static SubmissionKey from(JobStatusChangedEvent event) {
      return new SubmissionKey(
          event.jobId(),
          event instanceof SubmissionIdentity ? ((SubmissionIdentity) event).submissionId() : null);
    }
  }

  private final class JobState {
    private final OpenLineageContext context;
    private final LineageGraphConverter graphConverter;
    private final OpenLineageContinousJobTracker tracker;

    private JobState(SubmissionKey key) {
      OpenLineageContext.OpenLineageContextBuilder builder =
          OpenLineageJobStatusChangedListener.this.context.toBuilder().jobId(null);
      if (key.submissionId != null) {
        builder.runUuid(
            UUIDUtils.generateStaticUUID(
                Instant.EPOCH,
                (key.jobId.toHexString() + ":" + key.submissionId)
                    .getBytes(StandardCharsets.UTF_8)));
      }
      context = builder.build();
      graphConverter = new LineageGraphConverter(context, visitorFactory);
      tracker =
          new OpenLineageContinousJobTracker(
              Duration.ofSeconds(context.getConfig().getTrackingIntervalInSeconds()), jobsApiUrl);
    }

    private FlinkLineageFacet lineageStatus;
    private boolean startObserved;
    private volatile boolean finished;
    private final java.util.concurrent.atomic.AtomicBoolean trackingStopped =
        new java.util.concurrent.atomic.AtomicBoolean();

    private void stopTracking() {
      finished = true;
      if (trackingStopped.compareAndSet(false, true)) {
        try {
          tracker.stopTracking();
        } catch (Exception failure) {
          log.error("Stopping checkpoint tracker failed", failure);
        }
      }
    }
  }

  private void onJobCreatedEvent(JobState state, JobCreatedEvent event) {
    if (state.startObserved) {
      return;
    }
    state.startObserved = true;
    state.lineageStatus = FlinkLineageFacet.fromGraph(event.lineageGraph());
    loadJobId(state, event);
    RunEvent startEvent;
    try {
      startEvent = state.graphConverter.convert(event.lineageGraph(), EventType.START);
    } catch (Exception e) {
      log.error("Converting lineage failed; emitting unavailable observation", e);
      List<String> issues = new ArrayList<>(state.lineageStatus.getIssues());
      issues.add(
          "OpenLineage conversion failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      state.lineageStatus = new FlinkLineageFacet("UNAVAILABLE", "UNAVAILABLE", issues);
      OpenLineage openLineage = state.context.getOpenLineage();
      startEvent =
          commonEventBuilder(state)
              .eventType(EventType.START)
              .run(
                  openLineage
                      .newRunBuilder()
                      .runId(state.context.getRunUuid())
                      .facets(
                          openLineage
                              .newRunFacetsBuilder()
                              .processing_engine(buildProcessingEngineFacet(openLineage))
                              .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet(state))
                              .put("flink_lineage", state.lineageStatus)
                              .build())
                      .build())
              .inputs(Collections.emptyList())
              .outputs(Collections.emptyList())
              .build();
    }
    try {
      emit(state, startEvent);
    } catch (Exception e) {
      log.error("Emitting START event failed", e);
    }
    if (state.context.getConfig().getDisableCheckpointTracking()) {
      log.info("Checkpoint tracking is disabled via disableCheckpointTracking config");
    } else if (!state.finished) {
      state.tracker.startTracking(state.context, checkpoint -> onJobCheckpoint(state, checkpoint));
    }
  }

  private void onJobCheckpoint(JobState state, CheckpointFacet checkpointFacet) {
    synchronized (state) {
      if (state.finished) {
        return;
      }
      log.info("Emitting checkpoint event: {}", checkpointFacet);
      OpenLineage openLineage = state.context.getOpenLineage();
      RunEvent runEvent =
          commonEventBuilder(state)
              .eventType(EventType.RUNNING)
              .run(
                  openLineage
                      .newRunBuilder()
                      .runId(state.context.getRunUuid())
                      .facets(
                          openLineage
                              .newRunFacetsBuilder()
                              .processing_engine(buildProcessingEngineFacet(openLineage))
                              .put("checkpoints", checkpointFacet)
                              .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet(state))
                              .put("flink_lineage", state.lineageStatus)
                              .build())
                      .build())
              .build();

      if (log.isDebugEnabled()) {
        log.debug("Emitting checkpoint event: {}", OpenLineageClientUtils.toJson(runEvent));
      }
      try {
        emit(state, runEvent);
      } catch (Exception e) {
        log.error("Emitting checkpoint event failed", e);
      }
    }
  }

  private void onDefaultJobExecutionStatusEvent(
      JobState state, DefaultJobExecutionStatusEvent event) {
    EventType eventType = JobStatusUtil.fromJobStatus(event.newStatus());
    // Only JobCreatedEvent carries the graph needed for the authoritative START event.
    if (eventType == EventType.START) {
      return;
    }
    boolean terminal =
        List.of(EventType.COMPLETE, EventType.FAIL, EventType.ABORT).contains(eventType);
    try {
      if (state.context.getJobId() == null) {
        loadJobId(state, event);
        state.lineageStatus = FlinkLineageFacet.fromStatus(event.getLineageStatus());
      }

      OpenLineage openLineage = state.context.getOpenLineage();
      RunEvent runEvent =
          commonEventBuilder(state)
              .eventType(eventType)
              .run(
                  openLineage
                      .newRunBuilder()
                      .runId(state.context.getRunUuid())
                      .facets(
                          openLineage
                              .newRunFacetsBuilder()
                              .processing_engine(buildProcessingEngineFacet(openLineage))
                              .put(FLINK_JOB_FACET_KEY, buildJobDetailsFacet(state))
                              .put("flink_lineage", state.lineageStatus)
                              .build())
                      .build())
              .build();

      emit(state, runEvent);
    } finally {
      if (terminal) {
        state.stopTracking();
        jobs.remove(SubmissionKey.from(event), state);
        completedJobs.add(SubmissionKey.from(event));
      }
    }
  }

  private void emit(JobState state, RunEvent event) {
    try {
      delivery.execute(
          () -> {
            try {
              state.context.getEventEmitter().emit(event);
            } catch (Exception error) {
              log.error(
                  "OpenLineage delivery failed for run {} event {}; no adapter replay is available",
                  event.getRun().getRunId(),
                  event.getEventType(),
                  error);
            }
          });
    } catch (java.util.concurrent.RejectedExecutionException error) {
      log.error(
          "OpenLineage delivery queue full; dropping run {} event {}",
          event.getRun().getRunId(),
          event.getEventType());
    }
  }

  private RunEventBuilder commonEventBuilder(JobState state) {
    return state
        .context
        .getOpenLineage()
        .newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .job(
            state
                .context
                .getOpenLineage()
                .newJobBuilder()
                .namespace(state.context.getJobId().getJobNamespace())
                .name(state.context.getJobId().getJobName())
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

  private FlinkJobDetailsFacet buildJobDetailsFacet(JobState state) {
    JobIdentifier jobId = state.context.getJobId();
    if (jobId == null || jobId.getFlinkJobId() == null) {
      return null;
    }

    String flinkJobId = jobId.getFlinkJobId().toString();
    return new FlinkJobDetailsFacet(flinkJobId);
  }

  private void loadJobId(JobState state, JobStatusChangedEvent createdEvent) {
    String jobName =
        Optional.ofNullable(state.context.getConfig())
            .map(FlinkOpenLineageConfig::getJobConfig)
            .map(j -> j.getName())
            .orElse(createdEvent.jobName());

    String jobNamespace =
        Optional.ofNullable(state.context.getConfig())
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
    state.context.setJobId(jobId);
    // Legacy events cannot distinguish submissions that reuse a JobID.
    if (createdEvent.jobId() != null && SubmissionKey.from(createdEvent).submissionId == null) {
      state.context.setRunUuidFromFlinkJobId(createdEvent.jobId(), java.time.Instant.EPOCH);
    }
  }
}
