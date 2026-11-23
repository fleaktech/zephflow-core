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
package io.fleak.zephflow.lib.dlq;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.filedecode.Decodings;
import io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkCommandFactory;
import io.fleak.zephflow.lib.commands.stdin.StdInCommandFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** The writer a node creates counts what it drops on the node's own tags. */
class DlqWriterWiringTest {

  @Test
  void aSourceCountsDeadLettersItsWriterDroppedOnTheSourceTags() throws IOException {
    JobContext jobContext = jobContext(new HashMap<>());
    OperatorCommand command = new StdInCommandFactory().createCommand("source-node", jobContext);
    command.parseAndValidateArg(
        Map.of("decoding", Decodings.uncompressedConfig(Decodings.JSON_OBJECT)));

    assertEquals(
        Map.of(nodeTags(COMMAND_NAME_STDIN, "source-node"), 1L),
        notWrittenCountsAfterOneDroppedDeadLetter(command));
  }

  @Test
  void aSinkCountsDeadLettersItsWriterDroppedOnTheSinkTags() throws IOException {
    Map<String, Serializable> properties = new HashMap<>();
    properties.put("hec_token", new HashMap<>(Map.of("key", "token-" + UUID.randomUUID())));
    JobContext jobContext = jobContext(properties);
    OperatorCommand command =
        new SplunkHecSinkCommandFactory().createCommand("sink-node", jobContext);
    command.parseAndValidateArg(
        Map.of(
            "hecUrl", "http://127.0.0.1:1/services/collector/event",
            "credentialId", "hec_token",
            "batchSize", 2));

    assertEquals(
        Map.of(nodeTags(COMMAND_NAME_SPLUNK_HEC_SINK, "sink-node"), 1L),
        notWrittenCountsAfterOneDroppedDeadLetter(command));
  }

  private static Map<Map<String, String>, Long> notWrittenCountsAfterOneDroppedDeadLetter(
      OperatorCommand command) throws IOException {
    RecordingMetricClientProvider metrics = new RecordingMetricClientProvider();
    List<DlqWriter> createdWriters = new ArrayList<>();
    try (MockedStatic<DlqWriterFactory> factory =
        mockStatic(DlqWriterFactory.class, CALLS_REAL_METHODS)) {
      factory
          .when(() -> DlqWriterFactory.createDlqWriter(any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                DlqWriter writer = (DlqWriter) invocation.callRealMethod();
                createdWriters.add(writer);
                return writer;
              });
      command.initialize(metrics);
    }
    try {
      assertEquals(1, createdWriters.size());
      createdWriters
          .getFirst()
          .writeToDlq(1L, new SerializedEvent(null, "a".getBytes(), Map.of()), "error", "node");
      return metrics.totals(METRIC_NAME_DLQ_RECORDS_NOT_WRITTEN);
    } finally {
      command.terminate();
    }
  }

  private static JobContext jobContext(Map<String, Serializable> otherProperties)
      throws IOException {
    return JobContext.builder()
        .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
        .otherProperties(otherProperties)
        .dlqConfig(
            JobContext.S3DlqConfig.builder()
                .region("us-east-1")
                .bucket("dlq")
                .batchSize(1)
                .flushIntervalMillis(600_000)
                .accessKeyId("access-" + UUID.randomUUID())
                .secretAccessKey("x-" + UUID.randomUUID())
                .s3EndpointOverride(HungS3Server.refusingEndpoint())
                .build())
        .build();
  }

  private static Map<String, String> nodeTags(String commandName, String nodeId) {
    return Map.of(
        METRIC_TAG_SERVICE,
        "my_service",
        METRIC_TAG_ENV,
        "my_env",
        METRIC_TAG_COMMAND_NAME,
        commandName,
        METRIC_TAG_NODE_ID,
        nodeId);
  }

  private static final class RecordingMetricClientProvider
      extends MetricClientProvider.NoopMetricClientProvider {
    private final Map<String, Map<Map<String, String>, AtomicLong>> totals =
        new ConcurrentHashMap<>();

    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      AtomicLong total =
          totals
              .computeIfAbsent(name, ignored -> new ConcurrentHashMap<>())
              .computeIfAbsent(Map.copyOf(tags), ignored -> new AtomicLong());
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          total.incrementAndGet();
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          total.addAndGet(n);
        }
      };
    }

    Map<Map<String, String>, Long> totals(String name) {
      Map<Map<String, String>, Long> result = new HashMap<>();
      totals.getOrDefault(name, Map.of()).forEach((tags, total) -> result.put(tags, total.get()));
      return result;
    }
  }
}
