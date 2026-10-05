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
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionControl;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SplunkHecBoundedBatchTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void ordinaryAndBoundedCommandsHonorConfiguredBatchSize(boolean bounded) throws Exception {
    List<EffectOutcome> outcomes = new ArrayList<>();
    try (var receiver = new Receiver(200, new AtomicBoolean(), false)) {
      var command = command(receiver, bounded, () -> {}, outcomes);
      try {
        var result = command.writeToSink(records(), "user", command.getExecutionContext());
        assertEquals(5, result.getSuccessCount());
        assertTrue(result.getFailureEvents().isEmpty());
      } finally {
        command.terminate();
      }
      assertDelivery(receiver, 5, List.of(2, 2, 1));
      if (bounded) {
        assertEquals(1, outcomes.size());
        assertOutcome(outcomes.getFirst(), 5, 5, 0, 0, 0);
      } else assertTrue(outcomes.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 503, 0})
  void finalSliceFailureKeepsEarlierAcknowledgementsAndDoesNotRetry(int lastStatus)
      throws Exception {
    List<EffectOutcome> outcomes = new ArrayList<>();
    try (var receiver = new Receiver(lastStatus, new AtomicBoolean(), false)) {
      var command = command(receiver, true, () -> {}, outcomes);
      try {
        var result = command.writeToSink(records(), "user", command.getExecutionContext());
        assertEquals(4, result.getSuccessCount());
        assertEquals(1, result.getFailureEvents().size());
        assertEquals(records().getLast(), result.getFailureEvents().getFirst().inputEvent());
      } finally {
        command.abort();
      }
      assertDelivery(receiver, 5, List.of(2, 2, 1));
      assertEquals(1, outcomes.size());
      assertOutcome(
          outcomes.getFirst(), 5, 4, lastStatus == 400 ? 1 : 0, lastStatus == 400 ? 0 : 1, 0);
    }
  }

  @Test
  void cancellationBetweenSlicesPreservesAcknowledgementsAndDoesNotSendTail() throws Exception {
    AtomicBoolean cancelled = new AtomicBoolean();
    List<EffectOutcome> outcomes = new ArrayList<>();
    try (var receiver = new Receiver(200, cancelled, true)) {
      var command =
          command(
              receiver,
              true,
              () -> {
                if (cancelled.get())
                  throw new ExecutionStoppedException("cancelled between HEC slices");
              },
              outcomes);
      try {
        assertThrows(
            ExecutionStoppedException.class,
            () -> command.writeToSink(records(), "user", command.getExecutionContext()));
      } finally {
        command.abort();
      }
      assertDelivery(receiver, 4, List.of(2, 2));
      assertEquals(1, outcomes.size());
      assertOutcome(outcomes.getFirst(), 4, 4, 0, 0, 1);
    }
  }

  private static void assertOutcome(
      EffectOutcome outcome,
      long attempted,
      long acknowledged,
      long failed,
      long unknown,
      long notAttempted) {
    assertEquals(attempted, outcome.attemptedCount());
    assertEquals(acknowledged, outcome.acknowledgedCount());
    assertEquals(failed, outcome.definiteFailureCount());
    assertEquals(unknown, outcome.unknownCount());
    assertEquals(notAttempted, outcome.notAttemptedCount());
  }

  private static List<Map<String, String>> expectedEvents() {
    List<Map<String, String>> events = new ArrayList<>();
    for (int index = 1; index <= 5; index++) {
      events.add(Map.of("event_id", "login-" + index, "route", "audit", "message", "Zażółć"));
    }
    return events;
  }

  private static List<RecordFleakData> records() {
    return expectedEvents().stream().map(value -> (RecordFleakData) FleakData.wrap(value)).toList();
  }

  private static void assertDelivery(Receiver receiver, int count, List<Integer> batchSizes) {
    assertEquals(batchSizes, receiver.requests.stream().map(List::size).toList());
    var expected =
        expectedEvents().subList(0, count).stream()
            .map(event -> OBJECT_MAPPER.valueToTree(Map.of("event", event)))
            .toList();
    assertEquals(expected, receiver.requests.stream().flatMap(List::stream).toList());
  }

  private static SplunkHecSinkCommand command(
      Receiver receiver, boolean bounded, ExecutionControl control, List<EffectOutcome> outcomes)
      throws Exception {
    var properties = new HashMap<String, java.io.Serializable>();
    properties.put("hec-token", new HashMap<>(Map.of("key", "synthetic-batch-test")));
    properties.put(JobContext.FLAG_TEST_MODE, !bounded);
    var job =
        JobContext.builder()
            .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
            .otherProperties(properties)
            .build();
    var command =
        (SplunkHecSinkCommand) new SplunkHecSinkCommandFactory().createCommand("hec", job);
    command.parseAndValidateArg(
        Map.of("hecUrl", receiver.url(), "credentialId", "hec-token", "batchSize", 2));
    if (bounded)
      command.setExecutionHooks(
          new ExecutionHooks(
              control,
              new ExecutionHooks.Effects() {
                public long started() {
                  return 1;
                }

                public void finished(long id, EffectOutcome outcome) {
                  outcomes.add(outcome);
                }
              },
              Runnable::run));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return command;
  }

  private static final class Receiver implements AutoCloseable {
    private final HttpServer server;
    private final List<List<JsonNode>> requests = Collections.synchronizedList(new ArrayList<>());

    private Receiver(int lastStatus, AtomicBoolean cancelled, boolean cancelAfterSecond)
        throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/services/collector/event",
          exchange -> {
            List<JsonNode> events = new ArrayList<>();
            String body =
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            for (String line : body.lines().filter(value -> !value.isBlank()).toList()) {
              events.add(OBJECT_MAPPER.readTree(line));
            }
            requests.add(events);
            if (cancelAfterSecond && requests.size() == 2) cancelled.set(true);
            int status = requests.size() == 3 ? lastStatus : 200;
            if (status == 0) {
              exchange.close();
              return;
            }
            byte[] response =
                "{\"text\":\"controlled\",\"code\":0}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, response.length);
            try (var output = exchange.getResponseBody()) {
              output.write(response);
            }
          });
      server.start();
    }

    private String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event";
    }

    public void close() {
      server.stop(0);
    }
  }
}
