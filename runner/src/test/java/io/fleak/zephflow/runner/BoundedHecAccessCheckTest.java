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
package io.fleak.zephflow.runner;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class BoundedHecAccessCheckTest {
  @Test
  void runnerWrapsRuntimeAccessDenialAndPreservesFirstAcknowledgedHecSlice() throws Exception {
    var denied = new IllegalStateException("pinned credential no longer accessible");
    var accessDenied = new AtomicBoolean();
    List<List<JsonNode>> requests = Collections.synchronizedList(new ArrayList<>());
    var receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    receiver.createContext(
        "/services/collector/event",
        exchange -> {
          var received = new ArrayList<JsonNode>();
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          for (String line : body.lines().toList()) received.add(OBJECT_MAPPER.readTree(line));
          requests.add(received);
          accessDenied.set(true);
          byte[] response = "{\"code\":0}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, response.length);
          try (var output = exchange.getResponseBody()) {
            output.write(response);
          }
        });
    receiver.start();
    try {
      var service =
          new DagRunnerService(
              new DagCompiler(OperatorCommandRegistry.OPERATOR_COMMANDS),
              new MetricClientProvider.NoopMetricClientProvider());
      var node =
          new DagNode(
              "audit_sink",
              "splunkhecsink",
              Map.of(
                  "hecUrl",
                  "http://127.0.0.1:"
                      + receiver.getAddress().getPort()
                      + "/services/collector/event",
                  "credentialId",
                  "synthetic-hec",
                  "batchSize",
                  2),
              List.of());
      var properties = new HashMap<String, java.io.Serializable>();
      properties.put("synthetic-hec", new HashMap<>(Map.of("key", "test-only-token")));
      var runner =
          service.createForBoundedRun(
              List.of(node),
              JobContext.builder()
                  .metricTags(Map.of("service", "bounded-test", "env", "test"))
                  .otherProperties(properties)
                  .build(),
              new BoundedDefinition(Map.of(), Set.of("audit_sink")));
      var records = new ArrayList<RecordFleakData>();
      for (int index = 1; index <= 5; index++)
        records.add(
            (RecordFleakData)
                FleakData.wrap(Map.of("event_id", "login-" + index, "message", "Zażółć")));
      List<EffectOutcome> outcomes = new ArrayList<>();
      var observer = mock(ExecutionObserver.class);
      doAnswer(
              call -> {
                ExecutionObserver.Invocation invocation = call.getArgument(1);
                if (invocation.phase() == ExecutionObserver.Phase.PROCESS)
                  outcomes.add(call.getArgument(3));
                return null;
              })
          .when(observer)
          .effectFinished(anyLong(), any(), anyLong(), any());
      try {
        var stopped =
            assertThrows(
                ExecutionStoppedException.class,
                () ->
                    runner.runBounded(
                        List.of(
                            new BoundInput(
                                "bound-audit",
                                "audit_sink",
                                ExecutionObserver.Side.INPUT,
                                records)),
                        "user",
                        observer,
                        () -> {
                          if (accessDenied.get()) throw denied;
                        }));
        Throwable rootCause = stopped;
        while (rootCause.getCause() != null) rootCause = rootCause.getCause();
        assertSame(denied, rootCause);
        verify(observer, never()).invocationFailed(anyLong(), any(), any());
      } finally {
        runner.disposeBounded(CompletionDisposition.ABORTED);
      }
      assertEquals(List.of(2), requests.stream().map(List::size).toList());
      assertEquals(
          records.subList(0, 2).stream()
              .map(record -> OBJECT_MAPPER.valueToTree(Map.of("event", record.unwrap())))
              .toList(),
          requests.getFirst());
      assertEquals(1, outcomes.size());
      var receipt = outcomes.getFirst();
      assertEquals(2L, receipt.attemptedCount());
      assertEquals(2L, receipt.acknowledgedCount());
      assertEquals(0L, receipt.unknownCount());
      assertEquals(0L, receipt.definiteFailureCount());
      assertEquals(3L, receipt.notAttemptedCount());
    } finally {
      receiver.stop(0);
    }
  }
}
