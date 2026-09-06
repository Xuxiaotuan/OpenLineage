/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.circuitBreaker.CircuitBreaker;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.visitor.Flink2VisitorFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.DefaultJobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory.Context;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageGraphObservation;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class OpenLineageJobStatusChangedListenerTest {
  @Test
  @SneakyThrows
  void createdCallbackQueuedBeforeTerminalDoesNotRestartTracking() {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    java.util.List<Callable<?>> callbacks = new java.util.ArrayList<>();
    when(breaker.run(any()))
        .thenAnswer(
            invocation -> {
              callbacks.add(invocation.getArgument(0));
              return null;
            });
    OpenLineageContext template =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .circuitBreaker(breaker)
            .build();
    try (MockedConstruction<OpenLineageContinousJobTracker> trackers =
        mockConstruction(OpenLineageContinousJobTracker.class)) {
      listener = new OpenLineageJobStatusChangedListener(template, factory);
      JobID id = new JobID(1, 2);
      listener.onEvent(created(id, "job-a", "COMPLETE"));
      listener.onEvent(
          new DefaultJobExecutionStatusEvent(
              id, "job-a", JobStatus.RUNNING, JobStatus.FINISHED, null));
      callbacks.get(0).call();
      callbacks.get(1).call();
      assertThat(trackers.constructed()).hasSize(1);
      verify(trackers.constructed().get(0), times(1)).stopTracking();
      verify(trackers.constructed().get(0), times(0)).startTracking(any(), any());
    }
  }

  @Test
  @SneakyThrows
  void rejectedTerminalCallbackStillStopsTrackerAndSuppressesLateCreated() {
    CircuitBreaker breaker = mock(CircuitBreaker.class);
    java.util.concurrent.atomic.AtomicBoolean accept =
        new java.util.concurrent.atomic.AtomicBoolean(true);
    when(breaker.run(any()))
        .thenAnswer(
            invocation -> accept.get() ? ((Callable<?>) invocation.getArgument(0)).call() : null);
    OpenLineageContext template =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .circuitBreaker(breaker)
            .build();
    try (MockedConstruction<OpenLineageContinousJobTracker> trackers =
        mockConstruction(
            OpenLineageContinousJobTracker.class,
            (tracker, ignored) ->
                doThrow(new IllegalStateException("close failed")).when(tracker).stopTracking())) {
      listener = new OpenLineageJobStatusChangedListener(template, factory);
      JobID id = new JobID(1, 2);
      listener.onEvent(created(id, "job-a", "COMPLETE"));
      accept.set(false);
      listener.onEvent(
          new DefaultJobExecutionStatusEvent(
              id, "job-a", JobStatus.RUNNING, JobStatus.FINISHED, null));
      verify(trackers.constructed().get(0), times(1)).stopTracking();
      accept.set(true);
      listener.onEvent(created(id, "job-a", "COMPLETE"));
      assertThat(trackers.constructed()).hasSize(1);
    }
  }

  @Test
  void perJobStateDoesNotMutateSuppliedContextTemplate() {
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(context.getConfiguration());
    OpenLineageContext template = OpenLineageContextFactory.fromConfig(config).build();
    OpenLineageContext.JobIdentifier original =
        OpenLineageContext.JobIdentifier.builder()
            .jobName("template-job")
            .jobNamespace("template-ns")
            .build();
    template.setJobId(original);
    UUID originalRun = template.getRunUuid();
    listener = new OpenLineageJobStatusChangedListener(template, factory);
    listener.onEvent(created(new JobID(1, 2), "job-a", "COMPLETE"));
    listener.onEvent(created(new JobID(3, 4), "job-b", "UNAVAILABLE"));
    assertThat(template.getJobId()).isSameAs(original);
    assertThat(template.getRunUuid()).isEqualTo(originalRun);
  }

  @Test
  @SneakyThrows
  void terminalOnlyListenerRetainsPerSinkColumnStatuses() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);
    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2),
            "mixed-job",
            JobStatus.RUNNING,
            JobStatus.FINISHED,
            null,
            Map.of(
                DefaultJobExecutionStatusEvent.LINEAGE_TABLE_STATUS,
                "COMPLETE",
                DefaultJobExecutionStatusEvent.LINEAGE_COLUMN_STATUS,
                "PARTIAL",
                "internal.lineage.column-statuses",
                "{\"warehouse\":{\"good\":\"COMPLETE\",\"bad\":\"UNAVAILABLE\"}}")));
    RunEvent event =
        OpenLineageClientUtils.runEventFromJson(
            Files.readAllLines(Path.of(eventFileLocation)).get(0));
    com.fasterxml.jackson.databind.JsonNode status =
        OpenLineageClientUtils.newObjectMapper()
            .valueToTree(event)
            .path("run")
            .path("facets")
            .path("flink_lineage");
    assertThat(status.path("columnStatus").asText()).isEqualTo("PARTIAL");
    assertThat(status.path("columnStatuses"))
        .isEqualTo(
            OpenLineageClientUtils.newObjectMapper()
                .readTree("{\"warehouse\":{\"good\":\"COMPLETE\",\"bad\":\"UNAVAILABLE\"}}"));
  }

  @Test
  void checkpointFailureIsIsolatedAndLateCallbackIsIgnored() {
    java.util.List<java.util.function.Consumer<io.openlineage.flink.client.CheckpointFacet>>
        callbacks = new java.util.ArrayList<>();
    try (MockedConstruction<OpenLineageContinousJobTracker> trackers =
            mockConstruction(
                OpenLineageContinousJobTracker.class,
                (tracker, ignored) ->
                    doAnswer(
                            call -> {
                              callbacks.add(call.getArgument(1));
                              return null;
                            })
                        .when(tracker)
                        .startTracking(any(), any()));
        MockedConstruction<EventEmitter> emitters =
            mockConstruction(
                EventEmitter.class,
                (emitter, ignored) ->
                    doAnswer(
                            call -> {
                              RunEvent event = call.getArgument(0);
                              if (event.getEventType() == EventType.RUNNING)
                                throw new IllegalStateException("checkpoint transport failure");
                              return null;
                            })
                        .when(emitter)
                        .emit(any(RunEvent.class)))) {
      context.getConfiguration().setString("openlineage.disableCheckpointTracking", "false");
      listener = new OpenLineageJobStatusChangedListener(context, factory);
      listener.onEvent(created(new JobID(1, 2), "job", "COMPLETE"));
      io.openlineage.flink.client.CheckpointFacet checkpoint =
          new io.openlineage.flink.client.CheckpointFacet(1, 0, 0, 0, 1);
      assertThatCode(() -> callbacks.get(0).accept(checkpoint)).doesNotThrowAnyException();
      listener.onEvent(
          new DefaultJobExecutionStatusEvent(
              new JobID(1, 2), "job", JobStatus.RUNNING, JobStatus.FINISHED, null));
      callbacks.get(0).accept(checkpoint);
      verify(emitters.constructed().get(0), times(3)).emit(any(RunEvent.class));
      verify(trackers.constructed().get(0)).stopTracking();
    }
  }

  @Test
  void allTerminalStatesStopTrackerEvenWhenEmissionFails() {
    for (JobStatus terminal : List.of(JobStatus.FINISHED, JobStatus.FAILED, JobStatus.CANCELED)) {
      try (MockedConstruction<OpenLineageContinousJobTracker> trackers =
              mockConstruction(OpenLineageContinousJobTracker.class);
          MockedConstruction<EventEmitter> emitters =
              mockConstruction(
                  EventEmitter.class,
                  (emitter, ignored) ->
                      doAnswer(
                              call -> {
                                RunEvent event = call.getArgument(0);
                                if (event.getEventType() != EventType.START) {
                                  throw new IllegalStateException("terminal transport failure");
                                }
                                return null;
                              })
                          .when(emitter)
                          .emit(any(RunEvent.class)))) {
        context.getConfiguration().setString("openlineage.disableCheckpointTracking", "false");
        context.getConfiguration().setString("openlineage.disableCheckpointTracking", "false");
        listener = new OpenLineageJobStatusChangedListener(context, factory);
        listener.onEvent(created(new JobID(1, 2), "job", "COMPLETE"));
        assertThatCode(
                () ->
                    listener.onEvent(
                        new DefaultJobExecutionStatusEvent(
                            new JobID(1, 2), "job", JobStatus.RUNNING, terminal, null)))
            .doesNotThrowAnyException();
        verify(trackers.constructed().get(0), times(1)).stopTracking();
      }
    }
  }

  @Test
  @SneakyThrows
  void createdAndInitializingEmitOnlyOneStart() {
    try (MockedConstruction<OpenLineageContinousJobTracker> trackers =
        mockConstruction(OpenLineageContinousJobTracker.class)) {
      context.getConfiguration().setString("openlineage.disableCheckpointTracking", "false");
      listener = new OpenLineageJobStatusChangedListener(context, factory);
      JobCreatedEvent created = created(new JobID(1, 2), "job", "COMPLETE");
      listener.onEvent(created);
      listener.onEvent(created);
      listener.onEvent(
          new DefaultJobExecutionStatusEvent(
              new JobID(1, 2), "job", JobStatus.CREATED, JobStatus.INITIALIZING, null));
      List<RunEvent> events =
          Files.readAllLines(Path.of(eventFileLocation)).stream()
              .map(OpenLineageClientUtils::runEventFromJson)
              .collect(Collectors.toList());
      assertThat(events).extracting(RunEvent::getEventType).containsExactly(EventType.START);
      verify(trackers.constructed().get(0), times(1)).startTracking(any(), any());
    }
  }

  @Test
  @SneakyThrows
  void initializingBeforeCreatedDoesNotReplaceRichStart() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);
    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2), "job", JobStatus.CREATED, JobStatus.INITIALIZING, null));
    listener.onEvent(created(new JobID(1, 2), "job", "COMPLETE"));
    List<RunEvent> events =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());
    assertThat(events).extracting(RunEvent::getEventType).containsExactly(EventType.START);
    assertThat(OpenLineageClientUtils.toJson(events.get(0)))
        .contains("\"tableStatus\":\"COMPLETE\"");
  }

  @Test
  @SneakyThrows
  void interleavedJobsKeepTheirOwnIdentityStatusAndTracker() {
    try (MockedConstruction<OpenLineageContinousJobTracker> trackers =
        mockConstruction(OpenLineageContinousJobTracker.class)) {
      context.getConfiguration().setString("openlineage.disableCheckpointTracking", "false");
      listener = new OpenLineageJobStatusChangedListener(context, factory);
      JobID a = new JobID(1, 2), b = new JobID(3, 4);
      listener.onEvent(created(a, "job-a", "COMPLETE"));
      listener.onEvent(created(b, "job-b", "UNAVAILABLE"));
      listener.onEvent(
          new DefaultJobExecutionStatusEvent(
              a, "job-a", JobStatus.RUNNING, JobStatus.FINISHED, null));
      listener.onEvent(
          new DefaultJobExecutionStatusEvent(
              b, "job-b", JobStatus.RUNNING, JobStatus.FAILED, null));
      List<RunEvent> events =
          Files.readAllLines(Path.of(eventFileLocation)).stream()
              .map(OpenLineageClientUtils::runEventFromJson)
              .collect(Collectors.toList());
      assertThat(events)
          .extracting(e -> e.getJob().getName())
          .containsExactly("job-a", "job-b", "job-a", "job-b");
      assertThat(events.get(0).getRun().getRunId()).isEqualTo(events.get(2).getRun().getRunId());
      assertThat(events.get(1).getRun().getRunId()).isEqualTo(events.get(3).getRun().getRunId());
      assertThat(events.get(0).getRun().getRunId()).isNotEqualTo(events.get(1).getRun().getRunId());
      assertThat(OpenLineageClientUtils.toJson(events.get(2)))
          .contains("\"tableStatus\":\"COMPLETE\"");
      assertThat(OpenLineageClientUtils.toJson(events.get(3)))
          .contains("\"tableStatus\":\"UNAVAILABLE\"");
      assertThat(trackers.constructed()).hasSize(2);
      for (OpenLineageContinousJobTracker tracker : trackers.constructed())
        verify(tracker).stopTracking();
    }
  }

  private JobCreatedEvent created(JobID id, String name, String status) {
    JobCreatedEvent event = mock(JobCreatedEvent.class);
    when(event.jobId()).thenReturn(id);
    when(event.jobName()).thenReturn(name);
    when(event.lineageGraph())
        .thenReturn(
            new LineageGraphObservation(
                org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build(),
                status,
                status,
                List.of()));
    return event;
  }

  @Test
  @SneakyThrows
  void conversionFailurePublishesUnavailableStatusForWholeLifecycle() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);
    LineageGraph invalidGraph = mock(LineageGraph.class);
    when(invalidGraph.sources())
        .thenThrow(new IllegalArgumentException("invalid dataset identity"));
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("conversion-failed-job");
    when(createdEvent.lineageGraph())
        .thenReturn(new LineageGraphObservation(invalidGraph, "COMPLETE", "COMPLETE", List.of()));
    listener.onEvent(createdEvent);
    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2), "conversion-failed-job", JobStatus.RUNNING, JobStatus.FINISHED, null));
    List<RunEvent> events =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());
    assertThat(events)
        .extracting(RunEvent::getEventType)
        .containsExactly(EventType.START, EventType.COMPLETE);
    assertThat(events.get(0).getInputs()).isEmpty();
    assertThat(events.get(0).getOutputs()).isEmpty();
    assertThat(
            OpenLineageClientUtils.newObjectMapper()
                .valueToTree(events.get(0))
                .path("job")
                .path("facets")
                .has("lineage"))
        .isFalse();
    assertThat(events.get(0).getRun().getRunId()).isEqualTo(events.get(1).getRun().getRunId());
    for (RunEvent event : events) {
      com.fasterxml.jackson.databind.JsonNode status =
          OpenLineageClientUtils.newObjectMapper()
              .valueToTree(event)
              .path("run")
              .path("facets")
              .path("flink_lineage");
      assertThat(status.path("tableStatus").asText()).isEqualTo("UNAVAILABLE");
      assertThat(status.path("columnStatus").asText()).isEqualTo("UNAVAILABLE");
      assertThat(status.path("issues").toString()).contains("invalid dataset identity");
    }
  }

  @Test
  void transportFailureDoesNotRetryOrEscapeListener() {
    EventEmitter emitter = mock(EventEmitter.class);
    doThrow(new IllegalStateException("transport unavailable"))
        .when(emitter)
        .emit(any(RunEvent.class));
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(context.getConfiguration());
    config.setDisableCheckpointTracking(true);
    OpenLineageContext lineageContext =
        OpenLineageContextFactory.fromConfig(config).eventEmitter(emitter).build();
    listener = new OpenLineageJobStatusChangedListener(lineageContext, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("transport-failed-job");
    assertThatCode(() -> listener.onEvent(createdEvent)).doesNotThrowAnyException();
    verify(emitter, times(1)).emit(any(RunEvent.class));
  }

  @Test
  @SneakyThrows
  void missingLineagePublishesUnavailableStatusAndLifecycle() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("lineage-failed-job");
    listener.onEvent(createdEvent);
    listener.onEvent(
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2), "lineage-failed-job", JobStatus.RUNNING, JobStatus.FINISHED, null));
    List<RunEvent> events =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());
    assertThat(events).hasSize(2);
    assertThat(events)
        .extracting(RunEvent::getEventType)
        .containsExactly(EventType.START, EventType.COMPLETE);
    for (RunEvent event : events) {
      assertThat(OpenLineageClientUtils.toJson(event)).contains("\"columnStatus\":\"UNAVAILABLE\"");
    }
  }

  Context context = mock(Context.class, RETURNS_DEEP_STUBS);
  Flink2VisitorFactory factory = mock(Flink2VisitorFactory.class);
  OpenLineageJobStatusChangedListener listener;
  String eventFileLocation;

  @BeforeEach
  @SneakyThrows
  void setup() {
    if (!Files.isDirectory(Path.of("build/test_events"))) {
      Files.createDirectory(Path.of("build/test_events"));
    }
    eventFileLocation = "build/test_events/events_" + UUID.randomUUID();

    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation,
                "openlineage.disableCheckpointTracking",
                "true"));

    when(context.getConfiguration()).thenReturn(configuration);
  }

  @AfterEach
  void cleanup() throws IOException {
    Files.deleteIfExists(Path.of(eventFileLocation));
  }

  @Test
  @SneakyThrows
  void testOnEventForJobCreated() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");
    when(createdEvent.lineageGraph())
        .thenReturn(org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build());
    listener.onEvent(createdEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    RunEvent event =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList())
            .get(0);

    assertThat(event.getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(event.getJob().getName()).isEqualTo("event-job-name");
    assertThat(event.getRun().getRunId()).isNotNull();
    assertThat(event.getEventType()).isEqualTo(EventType.START);
  }

  @Test
  @SneakyThrows
  void testOnEventWithJobNameInConfig() {
    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation,
                "openlineage.job.name",
                "config-job-name"));

    when(context.getConfiguration()).thenReturn(configuration);

    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");
    when(createdEvent.lineageGraph())
        .thenReturn(org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build());
    listener.onEvent(createdEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    RunEvent event =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList())
            .get(0);

    assertThat(event.getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(event.getJob().getName()).isEqualTo("config-job-name");
    assertThat(event.getRun().getRunId()).isNotNull();
    assertThat(event.getEventType()).isEqualTo(EventType.START);
  }

  @Test
  @SneakyThrows
  void testOnEventWithJobNamespaceInConfig() {
    Configuration configuration =
        Configuration.fromMap(
            Map.of(
                "openlineage.transport.type",
                "file",
                "openlineage.transport.location",
                eventFileLocation,
                "openlineage.job.namespace",
                "flink://my.flink.domain:8081"));

    when(context.getConfiguration()).thenReturn(configuration);

    listener = new OpenLineageJobStatusChangedListener(context, factory);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");
    when(createdEvent.lineageGraph())
        .thenReturn(org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build());
    listener.onEvent(createdEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    RunEvent event =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList())
            .get(0);

    assertThat(event.getJob().getNamespace()).isEqualTo("flink://my.flink.domain:8081");
    assertThat(event.getJob().getName()).isEqualTo("event-job-name");
    assertThat(event.getRun().getRunId()).isNotNull();
    assertThat(event.getEventType()).isEqualTo(EventType.START);
  }

  @Test
  @SneakyThrows
  void testOnEventJobFinished() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);

    // emit start event
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");
    when(createdEvent.lineageGraph())
        .thenReturn(org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build());
    listener.onEvent(createdEvent);

    // emit complete event
    DefaultJobExecutionStatusEvent statusEvent =
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2),
            "jobName",
            JobStatus.RUNNING,
            JobStatus.FINISHED,
            mock(Throwable.class));
    listener.onEvent(statusEvent);

    Path path = Path.of(eventFileLocation);
    assertThat(Files.exists(path)).isTrue();

    List<RunEvent> eventsEmitted =
        Files.readAllLines(path).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(2);

    assertThat(eventsEmitted.get(0).getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(eventsEmitted.get(0).getJob().getName()).isEqualTo("event-job-name");
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.START);

    assertThat(eventsEmitted.get(1).getJob().getNamespace()).isEqualTo("flink-jobs");
    assertThat(eventsEmitted.get(1).getJob().getName()).isEqualTo("event-job-name");
    assertThat(eventsEmitted.get(1).getEventType()).isEqualTo(EventType.COMPLETE);
  }

  @Test
  @SneakyThrows
  void testOnEventJobFailed() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);

    // emit start event
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");
    when(createdEvent.lineageGraph())
        .thenReturn(org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build());
    listener.onEvent(createdEvent);

    // emit fail event
    DefaultJobExecutionStatusEvent statusEvent =
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2), "jobName", JobStatus.RUNNING, JobStatus.FAILED, mock(Throwable.class));
    listener.onEvent(statusEvent);

    List<RunEvent> eventsEmitted =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(2);
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.START);
    assertThat(eventsEmitted.get(1).getEventType()).isEqualTo(EventType.FAIL);
  }

  @Test
  @SneakyThrows
  void testOnEventJobCanceled() {
    listener = new OpenLineageJobStatusChangedListener(context, factory);

    // emit start event
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("event-job-name");
    when(createdEvent.lineageGraph())
        .thenReturn(org.apache.flink.streaming.api.lineage.DefaultLineageGraph.builder().build());
    listener.onEvent(createdEvent);

    // emit abort event
    DefaultJobExecutionStatusEvent statusEvent =
        new DefaultJobExecutionStatusEvent(
            new JobID(1, 2),
            "jobName",
            JobStatus.RUNNING,
            JobStatus.CANCELED,
            mock(Throwable.class));
    listener.onEvent(statusEvent);

    List<RunEvent> eventsEmitted =
        Files.readAllLines(Path.of(eventFileLocation)).stream()
            .map(OpenLineageClientUtils::runEventFromJson)
            .collect(Collectors.toList());

    assertThat(eventsEmitted).hasSize(2);
    assertThat(eventsEmitted.get(0).getEventType()).isEqualTo(EventType.START);
    assertThat(eventsEmitted.get(1).getEventType()).isEqualTo(EventType.ABORT);
  }

  @Test
  void testCircuitBreaker() {
    OpenLineageContext openLineageContext = mock(OpenLineageContext.class);
    CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
    when(openLineageContext.getCircuitBreaker()).thenReturn(circuitBreaker);
    listener = new OpenLineageJobStatusChangedListener(openLineageContext, factory);
    listener.onEvent(mock(JobCreatedEvent.class));
    verify(circuitBreaker, times(1)).run(any(Callable.class));
  }
}
