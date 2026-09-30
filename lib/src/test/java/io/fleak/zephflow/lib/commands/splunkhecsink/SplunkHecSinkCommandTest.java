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

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.TestUtils;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkHecSinkCommandTest {

  @Test
  void singleEventCalls_areBatchedIntoRequestsOfBatchSize() throws Exception {
    List<Integer> eventsPerRequest = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/services/collector/event",
        exchange -> {
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          eventsPerRequest.add((int) body.lines().filter(l -> !l.isEmpty()).count());
          byte[] out = "{\"text\":\"Success\",\"code\":0}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, out.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(out);
          }
        });
    server.start();

    JobContext jobContext =
        JobContext.builder()
            .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
            .otherProperties(
                new HashMap<>(Map.of("hec_token", new HashMap<>(Map.of("key", "abcd-1234")))))
            .build();
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder()
            .hecUrl(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event")
            .credentialId("hec_token")
            .batchSize(500)
            .build();

    SplunkHecSinkCommand command =
        (SplunkHecSinkCommand)
            new SplunkHecSinkCommandFactory().createCommand("splunkNode", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    long start = System.currentTimeMillis();
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      for (int i = 0; i < 1200; i++) {
        RecordFleakData event =
            new RecordFleakData(Map.of("seq", new StringPrimitiveFleakData(String.valueOf(i))));
        command.writeToSink(List.of(event), "test_user", command.getExecutionContext());
      }
    } finally {
      command.terminate();
      server.stop(0);
    }
    long elapsedMs = System.currentTimeMillis() - start;

    List<Integer> sizes = List.copyOf(eventsPerRequest);
    assertEquals(1200, sizes.stream().mapToInt(Integer::intValue).sum());
    assertTrue(sizes.stream().allMatch(n -> n <= 500), "request over batchSize: " + sizes);
    long timerTicks = 1 + elapsedMs / SplunkHecSinkCommand.FLUSH_INTERVAL_MS;
    assertTrue(sizes.size() <= 3 + timerTicks, "too many requests: " + sizes);
  }
}
