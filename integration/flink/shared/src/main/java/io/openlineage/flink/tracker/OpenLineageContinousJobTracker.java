/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.tracker;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.client.CheckpointFacet;
import io.openlineage.flink.tracker.restapi.Checkpoints;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.io.CloseMode;

@Slf4j
/**
 * Tracker class which spawns extra thread which call Flink Rest API endpoint to collect some
 * OpenLineage information.
 */
public class OpenLineageContinousJobTracker {

  private final Duration trackingInterval;
  private final String jobsApiUrl;

  private Consumer<CheckpointFacet> onJobCheckpoint;
  private volatile Thread trackingThread;
  private volatile CloseableHttpClient httpClient;
  private Optional<Checkpoints> latestCheckpoints = Optional.empty();
  private volatile boolean shouldContinue = true;

  public OpenLineageContinousJobTracker(Duration trackingInterval, String jobsApiUrl) {
    this.trackingInterval = trackingInterval;
    this.jobsApiUrl = jobsApiUrl;
  }

  /**
   * Starts tracking flink JOB rest API
   *
   * @param context flink execution context
   */
  public synchronized void startTracking(
      OpenLineageContext context, Consumer<CheckpointFacet> onJobCheckpoint) {
    if (!shouldContinue) {
      return;
    }
    this.onJobCheckpoint = onJobCheckpoint;

    if (context.getJobId() == null || context.getJobId().getFlinkJobId() == null) {
      log.error("Cannot start tracking thread, JobId is null. Can happen only in tests");
      return;
    }
    httpClient = HttpClients.createDefault();

    String url =
        String.format(
            "%s/%s/checkpoints", this.jobsApiUrl, context.getJobId().getFlinkJobId().toString());
    log.info("Tracking URL: {}", url);
    HttpGet request = new HttpGet(url);

    trackingThread =
        (new Thread(
            () -> {
              try (CloseableHttpClient client = httpClient) {
                try {
                  log.debug(
                      "Tracking thread started and sleeping {} seconds",
                      trackingInterval.getSeconds());
                  Thread.sleep(trackingInterval.toMillis());
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }

                while (shouldContinue) {
                  try (CloseableHttpResponse response = client.execute(request)) {
                    String json = EntityUtils.toString(response.getEntity());
                    log.debug("Tracking thread response: {}", json);

                    Optional.of(
                            new ObjectMapper()
                                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                .readValue(json, Checkpoints.class))
                        .filter(
                            newCheckpoints ->
                                latestCheckpoints.isEmpty()
                                    || latestCheckpoints.get().getCounts().getTotal()
                                        != newCheckpoints.getCounts().getTotal())
                        .ifPresentOrElse(
                            this::emitNewCheckpointEvent,
                            () -> log.info("no new checkpoint found"));
                  } catch (IOException | ParseException e) {
                    log.error("Connecting REST API failed", e);
                  } catch (Exception e) {
                    log.error("tracker thread failed due not unknown exception", e);
                    shouldContinue = false;
                  }
                  try {
                    if (!shouldContinue) {
                      break;
                    }
                    Thread.sleep(trackingInterval.toMillis());
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    shouldContinue = false;
                  }
                }
              } catch (IOException e) {
                log.warn("Closing checkpoint HTTP client failed", e);
              }
            }));
    log.info(
        "Starting tracking thread for jobId={}", context.getJobId().getFlinkJobId().toString());
    trackingThread.start();
  }

  private void emitNewCheckpointEvent(Checkpoints newCheckpoints) {
    log.info(
        "New checkpoint encountered total-checkpoint:{}", newCheckpoints.getCounts().getTotal());
    latestCheckpoints = Optional.of(newCheckpoints);

    onJobCheckpoint.accept(
        new CheckpointFacet(
            newCheckpoints.getCounts().getCompleted(),
            newCheckpoints.getCounts().getFailed(),
            newCheckpoints.getCounts().getIn_progress(),
            newCheckpoints.getCounts().getRestored(),
            newCheckpoints.getCounts().getTotal()));
  }

  /** Stops the tracking thread */
  public synchronized void stopTracking() {
    log.info("stop tracking");
    shouldContinue = false;
    Thread worker = trackingThread;
    if (worker != null) {
      worker.interrupt();
    }
    CloseableHttpClient client = httpClient;
    if (client != null) {
      client.close(CloseMode.IMMEDIATE);
    }
  }
}
