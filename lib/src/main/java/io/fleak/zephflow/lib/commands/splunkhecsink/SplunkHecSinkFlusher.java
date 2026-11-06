/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.AbstractBufferedFlusher;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class SplunkHecSinkFlusher extends AbstractBufferedFlusher<SplunkHecOutboundEvent> {

  private static final int MAX_BODY_SNIPPET = 500;

  private final String hecUrl;
  private final String authHeader;
  private final HttpClient httpClient;
  private final int batchSize;
  private final long flushIntervalMs;

  public SplunkHecSinkFlusher(
      String hecUrl,
      String hecToken,
      HttpClient httpClient,
      int batchSize,
      long flushIntervalMs,
      DlqWriter dlqWriter,
      JobContext jobContext,
      String nodeId,
      FleakCounter sinkOutputCounter,
      FleakCounter outputSizeCounter,
      FleakCounter sinkErrorCounter) {
    super(dlqWriter, jobContext, nodeId, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
    this.hecUrl = hecUrl;
    this.authHeader = "Splunk " + hecToken;
    this.httpClient = httpClient;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
  }

  @Override
  protected int getBatchSize() {
    return batchSize;
  }

  @Override
  protected long getFlushIntervalMs() {
    return flushIntervalMs;
  }

  @Override
  protected String getSchedulerThreadName() {
    return "splunk-hec-flusher-timer";
  }

  @Override
  protected void ensureCanWriteRecord(SplunkHecOutboundEvent record) {}

  @Override
  protected SimpleSinkCommand.FlushResult doFlushWithRecovery(
      List<Pair<RecordFleakData, SplunkHecOutboundEvent>> batch) {
    List<SimpleSinkCommand.FlushResult> results = new ArrayList<>();
    Exception storedFailure = null;
    for (List<Pair<RecordFleakData, SplunkHecOutboundEvent>> slice :
        Lists.partition(batch, batchSize)) {
      // once a slice is stored, store the rest behind it so HEC receives them in order
      Exception failure = storedFailure;
      if (failure == null) {
        try {
          results.add(doFlushWithRetry(slice));
          continue;
        } catch (Exception e) {
          failure = e;
        }
      }
      Optional<SimpleSinkCommand.FlushResult> stored = storeOnDestinationFailure(slice, failure);
      if (stored.isPresent()) {
        storedFailure = failure;
        results.add(stored.get());
      } else {
        results.add(createCompleteFailureResult(slice, failureMessage(failure)));
      }
    }
    return merge(results);
  }

  private static SimpleSinkCommand.FlushResult merge(List<SimpleSinkCommand.FlushResult> results) {
    int successCount = 0;
    long flushedDataSize = 0;
    List<ErrorOutput> errors = new ArrayList<>();
    for (SimpleSinkCommand.FlushResult result : results) {
      successCount += result.successCount();
      flushedDataSize += result.flushedDataSize();
      errors.addAll(result.errorOutputList());
    }
    return new SimpleSinkCommand.FlushResult(successCount, flushedDataSize, errors);
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlushWithRetry(
      List<Pair<RecordFleakData, SplunkHecOutboundEvent>> slice) throws Exception {
    long retryDelayMs = INITIAL_RETRY_DELAY_MS;
    try {
      for (int attempt = 1; ; attempt++) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("Splunk HEC flush interrupted");
        }
        try {
          return doFlush(slice);
        } catch (HecResponseException | IOException e) {
          if (attempt >= MAX_WRITE_RETRIES
              || e instanceof HecResponseException hec && !hec.isRetryable()) {
            throw e;
          }
          log.warn(
              "Splunk HEC error (attempt {}/{}), retrying in {}ms: {}",
              attempt,
              MAX_WRITE_RETRIES,
              retryDelayMs,
              failureMessage(e));
          Thread.sleep(retryDelayMs);
          retryDelayMs *= 2;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlush(
      List<Pair<RecordFleakData, SplunkHecOutboundEvent>> slice) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    for (Pair<RecordFleakData, SplunkHecOutboundEvent> event : slice) {
      buf.write(event.getRight().preEncodedNdjsonLine());
    }
    byte[] bodyBytes = buf.toByteArray();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(hecUrl))
            .header("Authorization", authHeader)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(Duration.ofSeconds(60))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    int status = response.statusCode();
    if (status == 200) {
      log.debug("Successfully sent {} events to Splunk HEC", slice.size());
      return new SimpleSinkCommand.FlushResult(slice.size(), bodyBytes.length, List.of());
    }

    HecResponseException failure = HecResponseException.fromResponse(status, response.body());
    String reason = failure.getMessage();
    if (boundedHooks != null) {
      boolean rejected = status >= 400 && status < 500 && status != 408;
      return new SimpleSinkCommand.FlushResult(
          0,
          0,
          slice.stream().map(event -> new ErrorOutput(event.getLeft(), reason)).toList(),
          EffectOutcome.counted(
              slice.size(),
              0,
              rejected ? slice.size() : 0,
              rejected ? 0 : slice.size(),
              0,
              "hec_http_" + status));
    }
    if (!failure.isRetryable()) {
      log.error("Splunk HEC returned status {}: {}", status, response.body());
    }
    throw failure;
  }

  private static String failureMessage(Exception e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }

  @Override
  public void close() {
    stopFlushTimer();
    if (dlqWriter != null) {
      try {
        dlqWriter.close();
      } catch (Exception e) {
        log.warn("Failed to close DLQ writer", e);
      }
    }
  }

  /** A non-200 HEC response, keeping the HTTP status and the HEC {@code code} of its body. */
  @Getter
  static final class HecResponseException extends Exception {
    private final int status;
    private final Integer hecCode;

    private HecResponseException(int status, Integer hecCode, String message) {
      super(message);
      this.status = status;
      this.hecCode = hecCode;
    }

    static HecResponseException fromResponse(int status, String body) {
      String text = body == null ? "" : body;
      String reason = text.length() > MAX_BODY_SNIPPET ? text.substring(0, MAX_BODY_SNIPPET) : text;
      Integer hecCode = null;
      try {
        JsonNode node = OBJECT_MAPPER.readTree(text);
        if (node != null && node.get("text") != null && node.get("text").isTextual()) {
          reason = node.get("text").asText();
        }
        if (node != null && node.get("code") != null && node.get("code").isInt()) {
          hecCode = node.get("code").asInt();
        }
      } catch (Exception ignore) {
        // not a HEC JSON body; keep the raw snippet
      }
      return new HecResponseException(
          status, hecCode, "Splunk HEC error " + status + ": " + reason);
    }

    boolean isRetryable() {
      return status == 429 || status >= 500;
    }
  }
}
