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
  void collectorFailureDoesNotReplayStartOrPreventLaterDelivery() throws Exception {
    java.util.concurrent.BlockingQueue<RunEvent> received =
        new java.util.concurrent.LinkedBlockingQueue<>();
    java.util.concurrent.atomic.AtomicInteger response =
        new java.util.concurrent.atomic.AtomicInteger(400);
    com.sun.net.httpserver.HttpServer server =
        com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/api/v1/lineage",
        exchange -> {
          RunEvent event =
              OpenLineageClientUtils.runEventFromJson(
                  new String(
                      exchange.getRequestBody().readAllBytes(),
                      java.nio.charset.StandardCharsets.UTF_8));
          exchange.sendResponseHeaders(response.get(), -1);
          exchange.close();
          received.add(event);
        });
    server.start();
    try {
      Configuration configuration = context.getConfiguration();
      configuration.setString("openlineage.transport.type", "http");
      configuration.setString(
          "openlineage.transport.url", "http://127.0.0.1:" + server.getAddress().getPort());
      configuration.setString("openlineage.transport.timeoutInMillis", "500");
      listener = new OpenLineageJobStatusChangedListener(context, factory);
      JobID id = new JobID();
      listener.onEvent(identifiedCreated(id, "failed-start"));
      assertThat(received.poll(10, java.util.concurrent.TimeUnit.SECONDS).getEventType())
          .isEqualTo(EventType.START);
      response.set(200);
      listener.onEvent(identifiedTerminal(id, "failed-start"));
      listener.onEvent(identifiedTerminal(id, "failed-start"));
      listener.onEvent(identifiedCreated(id, "next-run"));
      RunEvent terminal = received.poll(10, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(terminal.getEventType()).isEqualTo(EventType.COMPLETE);
      assertThat(terminal.getOutputs()).isNullOrEmpty();
      assertThat(received.poll(10, java.util.concurrent.TimeUnit.SECONDS).getEventType())
          .isEqualTo(EventType.START);
      assertThat(received).isEmpty();
    } finally {
      server.stop(0);
    }
  }

  @Test
  void slowTransportDoesNotHoldListenerCallback() throws Exception {
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    EventEmitter emitter = mock(EventEmitter.class);
    doAnswer(
            call -> {
              entered.countDown();
              release.await(10, java.util.concurrent.TimeUnit.SECONDS);
              return null;
            })
        .when(emitter)
        .emit(any(RunEvent.class));
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(context.getConfiguration());
    OpenLineageContext lineageContext =
        OpenLineageContextFactory.fromConfig(config).eventEmitter(emitter).build();
    listener = new OpenLineageJobStatusChangedListener(lineageContext, factory);
    java.util.concurrent.ExecutorService caller =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      java.util.concurrent.Future<?> callback =
          caller.submit(() -> listener.onEvent(identifiedCreated(new JobID(), "slow")));
      assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      callback.get(1, java.util.concurrent.TimeUnit.SECONDS);
      caller
          .submit(() -> listener.onEvent(identifiedCreated(new JobID(), "other-job")))
          .get(1, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      release.countDown();
      caller.shutdownNow();
    }
  }

  @Test
  void fullDeliveryQueueDoesNotRunTransportOnCaller() throws Exception {
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch delivered = new java.util.concurrent.CountDownLatch(2);
    java.util.List<String> deliveryThreads = new java.util.concurrent.CopyOnWriteArrayList<>();
    EventEmitter emitter = mock(EventEmitter.class);
    doAnswer(
            call -> {
              deliveryThreads.add(Thread.currentThread().getName());
              entered.countDown();
              release.await(10, java.util.concurrent.TimeUnit.SECONDS);
              delivered.countDown();
              return null;
            })
        .when(emitter)
        .emit(any(RunEvent.class));
    java.util.concurrent.ThreadPoolExecutor executor =
        new java.util.concurrent.ThreadPoolExecutor(
            1,
            1,
            1,
            java.util.concurrent.TimeUnit.SECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(1),
            task -> new Thread(task, "test-delivery"));
    OpenLineageContext lineageContext =
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .eventEmitter(emitter)
            .build();
    listener = new OpenLineageJobStatusChangedListener(lineageContext, factory, executor);
    try {
      listener.onEvent(identifiedCreated(new JobID(), "first"));
      assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      listener.onEvent(identifiedCreated(new JobID(), "queued"));
      assertThatCode(() -> listener.onEvent(identifiedCreated(new JobID(), "rejected")))
          .doesNotThrowAnyException();
      release.countDown();
      assertThat(delivered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
      assertThat(deliveryThreads).containsExactly("test-delivery", "test-delivery");
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  @SneakyThrows
  void lateCheckpointFromPreviousSubmissionCannotEmitIntoNextRun() {
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
                    .startTracking(any(), any()))) {
      context.getConfiguration().setString("openlineage.disableCheckpointTracking", "false");
      listener = synchronousListener();
      JobID id = new JobID(1, 2);
      listener.onEvent(identifiedCreated(id, "first"));
      listener.onEvent(identifiedTerminal(id, "first"));
      listener.onEvent(identifiedCreated(id, "second"));
      callbacks.get(0).accept(new io.openlineage.flink.client.CheckpointFacet(1, 0, 0, 0, 1));
      listener.onEvent(identifiedTerminal(id, "second"));
      assertThat(readEvents())
          .extracting(RunEvent::getEventType)
          .containsExactly(
              EventType.START, EventType.COMPLETE, EventType.START, EventType.COMPLETE);
    }
  }

  @Test
  @SneakyThrows
  void reusedJobIdKeepsSubmissionLifecyclesAndLateEventsSeparate() {
    listener = synchronousListener();
    JobID id = new JobID(1, 2);
    listener.onEvent(identifiedCreated(id, "first"));
    listener.onEvent(identifiedTerminal(id, "first"));
    listener.onEvent(identifiedCreated(id, "second"));
    listener.onEvent(identifiedTerminal(id, "first"));
    listener.onEvent(identifiedCreated(id, "first"));
    listener.onEvent(identifiedTerminal(id, "second"));
    List<RunEvent> events = readEvents();
    assertThat(events)
        .extracting(RunEvent::getEventType)
        .containsExactly(EventType.START, EventType.COMPLETE, EventType.START, EventType.COMPLETE);
    assertThat(events.get(0).getRun().getRunId()).isEqualTo(events.get(1).getRun().getRunId());
    assertThat(events.get(2).getRun().getRunId()).isEqualTo(events.get(3).getRun().getRunId());
    assertThat(events.get(0).getRun().getRunId()).isNotEqualTo(events.get(2).getRun().getRunId());
  }

  @Test
  @SneakyThrows
  void separateClientAndJobManagerListenersAgreeAcrossRecovery() {
    JobID id = new JobID(1, 2);
    for (String submission : List.of("first", "second")) {
      synchronousListener().onEvent(identifiedCreated(id, submission));
      synchronousListener().onEvent(identifiedTerminal(id, submission));
    }
    List<RunEvent> events = readEvents();
    assertThat(events).hasSize(4);
    assertThat(events.get(0).getRun().getRunId()).isEqualTo(events.get(1).getRun().getRunId());
    assertThat(events.get(2).getRun().getRunId()).isEqualTo(events.get(3).getRun().getRunId());
    assertThat(events.get(0).getRun().getRunId()).isNotEqualTo(events.get(2).getRun().getRunId());
  }

  private JobCreatedEvent identifiedCreated(JobID id, String submission) {
    return new org.apache.flink.streaming.runtime.execution.DefaultJobCreatedEvent(
        id,
        "reused-job",
        created(id, "reused-job", "COMPLETE").lineageGraph(),
        org.apache.flink.api.common.RuntimeExecutionMode.BATCH,
        submission);
  }

  private DefaultJobExecutionStatusEvent identifiedTerminal(JobID id, String submission) {
    return new DefaultJobExecutionStatusEvent(
        id, "reused-job", JobStatus.RUNNING, JobStatus.FINISHED, null, Map.of(), submission);
  }

  private OpenLineageJobStatusChangedListener synchronousListener() {
    return new OpenLineageJobStatusChangedListener(
        OpenLineageContextFactory.fromConfig(FlinkConfigParser.parse(context.getConfiguration()))
            .build(),
        factory,
        Runnable::run);
  }

  private List<RunEvent> readEvents() throws IOException {
    return Files.readAllLines(Path.of(eventFileLocation)).stream()
        .map(OpenLineageClientUtils::runEventFromJson)
        .collect(Collectors.toList());
  }

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
      listener = new OpenLineageJobStatusChangedListener(template, factory, Runnable::run);
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
      listener = new OpenLineageJobStatusChangedListener(template, factory, Runnable::run);
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
    listener = new OpenLineageJobStatusChangedListener(template, factory, Runnable::run);
    listener.onEvent(created(new JobID(1, 2), "job-a", "COMPLETE"));
    listener.onEvent(created(new JobID(3, 4), "job-b", "UNAVAILABLE"));
    assertThat(template.getJobId()).isSameAs(original);
    assertThat(template.getRunUuid()).isEqualTo(originalRun);
  }

  @Test
  @SneakyThrows
  void terminalOnlyListenerRetainsPerSinkColumnStatuses() {
    listener = synchronousListener();
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
                DefaultJobExecutionStatusEvent.LINEAGE_TABLE_STATUSES,
                "{\"warehouse\":{\"good\":\"COMPLETE\",\"bad\":\"UNAVAILABLE\"}}",
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
    assertThat(status.path("tableStatuses")).isEqualTo(status.path("columnStatuses"));
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
      listener = synchronousListener();
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
        listener = synchronousListener();
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
      listener = synchronousListener();
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
    listener = synchronousListener();
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
      listener = synchronousListener();
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
    listener = synchronousListener();
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
    listener = new OpenLineageJobStatusChangedListener(lineageContext, factory, Runnable::run);
    JobCreatedEvent createdEvent = mock(JobCreatedEvent.class);
    when(createdEvent.jobId()).thenReturn(new JobID(1, 2));
    when(createdEvent.jobName()).thenReturn("transport-failed-job");
    assertThatCode(() -> listener.onEvent(createdEvent)).doesNotThrowAnyException();
    verify(emitter, times(1)).emit(any(RunEvent.class));
  }

  @Test
  @SneakyThrows
  void missingLineagePublishesUnavailableStatusAndLifecycle() {
    listener = synchronousListener();
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
    listener = synchronousListener();
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

    listener = synchronousListener();
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

    listener = synchronousListener();
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
    listener = synchronousListener();

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
    listener = synchronousListener();

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
    listener = synchronousListener();

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
