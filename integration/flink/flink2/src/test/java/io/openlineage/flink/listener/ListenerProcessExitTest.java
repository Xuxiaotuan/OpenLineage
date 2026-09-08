/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.EventEmitter;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.lineage.DefaultLineageGraph;
import org.apache.flink.streaming.runtime.execution.DefaultJobCreatedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ListenerProcessExitTest {
  @TempDir Path temporary;

  @Test
  void stuckDeliveryDoesNotPreventExit() throws Exception {
    Path output = temporary.resolve("stuck.log");
    Process child =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("listener.test.classpath"),
                ListenerProcessExitTest.class.getName(),
                "http://127.0.0.1:1",
                "60000")
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertThat(child.waitFor(15, TimeUnit.SECONDS)).as("shutdown must be bounded").isTrue();
      assertThat(child.exitValue()).isZero();
      assertThat(Files.readString(output)).contains("OpenLineage shutdown drain did not complete");
    } finally {
      if (child.isAlive()) child.destroyForcibly();
    }
  }

  @Test
  void normalExitDrainsPendingStart() throws Exception {
    CountDownLatch received = new CountDownLatch(1);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/api/v1/lineage",
        exchange -> {
          String body =
              new String(
                  exchange.getRequestBody().readAllBytes(),
                  java.nio.charset.StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
          if (body.contains("\"eventType\":\"START\"")) received.countDown();
        });
    server.start();
    Process child = null;
    try {
      child =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  System.getProperty("listener.test.classpath"),
                  ListenerProcessExitTest.class.getName(),
                  "http://127.0.0.1:" + server.getAddress().getPort(),
                  "500")
              .redirectErrorStream(true)
              .redirectOutput(temporary.resolve("child.log").toFile())
              .start();
      assertThat(child.waitFor(15, TimeUnit.SECONDS)).isTrue();
      assertThat(child.exitValue()).isZero();
      assertThat(received.await(2, TimeUnit.SECONDS))
          .as("START must arrive despite immediate JVM exit")
          .isTrue();
    } finally {
      if (child != null && child.isAlive()) child.destroyForcibly();
      server.stop(0);
    }
  }

  public static void main(String[] args) {
    Configuration configuration = new Configuration();
    configuration.setString("openlineage.transport.type", "http");
    configuration.setString("openlineage.transport.url", args[0]);
    configuration.setString("openlineage.disableCheckpointTracking", "true");
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(configuration);
    EventEmitter delayed =
        new EventEmitter(config) {
          @Override
          public void emit(RunEvent event) {
            try {
              Thread.sleep(Long.parseLong(args[1]));
            } catch (InterruptedException failure) {
              Thread.currentThread().interrupt();
              return;
            }
            super.emit(event);
          }
        };
    OpenLineageJobStatusChangedListener listener =
        new OpenLineageJobStatusChangedListener(
            OpenLineageContextFactory.fromConfig(config).eventEmitter(delayed).build(),
            new OpenLineageJobStatusChangedListenerFactory().loadVisitorFactory());
    listener.onEvent(
        new DefaultJobCreatedEvent(
            new JobID(),
            "short-lived-client",
            DefaultLineageGraph.builder().build(),
            RuntimeExecutionMode.BATCH,
            "exit-test"));
    System.exit(0);
  }
}
