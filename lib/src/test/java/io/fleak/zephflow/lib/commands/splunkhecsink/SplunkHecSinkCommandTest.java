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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.ScalarSinkCommand.SinkResult;
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

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

  @Test
  void csvEncoding_sendsOrderedCsvLinesFromNodeConfig() throws Exception {
    List<String> bodies = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/services/collector/event",
        exchange -> {
          bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
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
    Map<String, Object> nodeConfig = new HashMap<>();
    nodeConfig.put(
        "hecUrl",
        "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event");
    nodeConfig.put("credentialId", "hec_token");
    nodeConfig.put("sourcetype", "fleak:csv");
    nodeConfig.put("encodingType", "CSV");
    nodeConfig.put("csvColumns", List.of("time", "src_ip", "action"));

    SplunkHecSinkCommand command =
        (SplunkHecSinkCommand)
            new SplunkHecSinkCommandFactory().createCommand("splunkNode", jobContext);
    command.parseAndValidateArg(nodeConfig);

    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      command.writeToSink(
          List.of(
              new RecordFleakData(
                  Map.of(
                      "action", new StringPrimitiveFleakData("allow"),
                      "time", new StringPrimitiveFleakData("2026-10-19"),
                      "src_ip", new StringPrimitiveFleakData("1.2.3.4"))),
              new RecordFleakData(
                  Map.of(
                      "time", new StringPrimitiveFleakData("2026-10-20"),
                      "user", new StringPrimitiveFleakData("dropped")))),
          "test_user",
          command.getExecutionContext());
    } finally {
      command.terminate();
      server.stop(0);
    }

    List<JsonNode> envelopes = new ArrayList<>();
    for (String body : List.copyOf(bodies)) {
      for (String line : body.lines().filter(l -> !l.isEmpty()).toList()) {
        envelopes.add(OBJECT_MAPPER.readTree(line));
      }
    }
    assertEquals(2, envelopes.size());
    assertEquals("2026-10-19,1.2.3.4,allow", envelopes.get(0).get("event").asText());
    assertEquals("2026-10-20,,", envelopes.get(1).get("event").asText());
    assertEquals("fleak:csv", envelopes.get(0).get("sourcetype").asText());
  }

  @Test
  void kvEncoding_sendsKeyValueLinesAndRejectsEventsWithNoFields() throws Exception {
    List<String> bodies = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/services/collector/event",
        exchange -> {
          bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
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
    Map<String, Object> nodeConfig = new HashMap<>();
    nodeConfig.put(
        "hecUrl",
        "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event");
    nodeConfig.put("credentialId", "hec_token");
    nodeConfig.put("sourcetype", "fleak:kv");
    nodeConfig.put("encodingType", "KV");
    nodeConfig.put("kvFields", List.of("time", "src_ip", "action"));
    nodeConfig.put("kvPairDelimiter", "|");

    SplunkHecSinkCommand command =
        (SplunkHecSinkCommand)
            new SplunkHecSinkCommandFactory().createCommand("splunkNode", jobContext);
    command.parseAndValidateArg(nodeConfig);

    ScalarSinkCommand.SinkResult result;
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      result =
          command.writeToSink(
              List.of(
                  new RecordFleakData(
                      Map.of(
                          "action", new StringPrimitiveFleakData("allow"),
                          "time", new StringPrimitiveFleakData("2026-10-19"),
                          "src_ip", new StringPrimitiveFleakData("1.2.3.4"))),
                  new RecordFleakData(
                      Map.of(
                          "time", new StringPrimitiveFleakData("2026-10-20"),
                          "user", new StringPrimitiveFleakData("dropped"))),
                  new RecordFleakData(Map.of("user", new StringPrimitiveFleakData("no kv field")))),
              "test_user",
              command.getExecutionContext());
    } finally {
      command.terminate();
      server.stop(0);
    }

    List<JsonNode> envelopes = new ArrayList<>();
    for (String body : List.copyOf(bodies)) {
      for (String line : body.lines().filter(l -> !l.isEmpty()).toList()) {
        envelopes.add(OBJECT_MAPPER.readTree(line));
      }
    }
    assertEquals(3, result.getInputCount());
    assertEquals(1, result.getFailureEvents().size());
    assertTrue(
        result
            .getFailureEvents()
            .get(0)
            .errorMessage()
            .contains("none of the configured KV fields"),
        result.getFailureEvents().get(0).errorMessage());
    assertEquals(2, envelopes.size());
    assertEquals(
        "time=2026-10-19|src_ip=1.2.3.4|action=allow", envelopes.get(0).get("event").asText());
    assertEquals("time=2026-10-20", envelopes.get(1).get("event").asText());
    assertEquals("fleak:kv", envelopes.get(0).get("sourcetype").asText());
  }

  private static final RecordFleakData RECORD =
      new RecordFleakData(
          Map.of(
              "a", new StringPrimitiveFleakData("1"), "b", new StringPrimitiveFleakData("x, y")));

  @Test
  void encodingJsonObject_sendsTheDefaultEnvelope() throws Exception {
    List<String> expected =
        List.of("{\"event\":{\"a\":\"1\",\"b\":\"x, y\"},\"sourcetype\":\"fleak\"}");

    assertEquals(expected, sentEnvelopes(Map.of()));
    assertEquals(
        expected,
        sentEnvelopes(
            Map.of("encoding", Map.of("format", Map.of("type", "json", "mode", "object")))));
  }

  @Test
  void encodingCsv_sendsTheEnvelopeOfCsvColumns() throws Exception {
    List<String> expected = List.of("{\"event\":\"1,\\\"x, y\\\"\",\"sourcetype\":\"fleak\"}");

    assertEquals(
        expected, sentEnvelopes(Map.of("encodingType", "CSV", "csvColumns", List.of("a", "b"))));
    assertEquals(
        expected,
        sentEnvelopes(
            Map.of(
                "encoding",
                Map.of(
                    "format",
                    Map.of("type", "csv", "header", false, "columns", List.of("a", "b"))))));
  }

  @Test
  void encodingKv_sendsTheEnvelopeOfKvOptions() throws Exception {
    List<String> expected = List.of("{\"event\":\"b=\\\"x, y\\\"|a=1\",\"sourcetype\":\"fleak\"}");

    assertEquals(
        expected,
        sentEnvelopes(
            Map.of("encodingType", "KV", "kvFields", List.of("b", "a"), "kvPairDelimiter", "|")));
    assertEquals(
        expected,
        sentEnvelopes(
            Map.of(
                "encoding",
                Map.of(
                    "format",
                    Map.of("type", "kv", "fields", List.of("b", "a"), "pairDelimiter", "|")))));
  }

  private static List<String> sentEnvelopes(Map<String, Object> format) throws Exception {
    List<String> bodies = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/services/collector/event",
        exchange -> {
          bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
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
    Map<String, Object> nodeConfig = new HashMap<>(format);
    nodeConfig.put(
        "hecUrl",
        "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event");
    nodeConfig.put("credentialId", "hec_token");
    nodeConfig.put("sourcetype", "fleak");

    SplunkHecSinkCommand command =
        (SplunkHecSinkCommand)
            new SplunkHecSinkCommandFactory().createCommand("splunkNode", jobContext);
    command.parseAndValidateArg(nodeConfig);
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      command.writeToSink(List.of(RECORD), "test_user", command.getExecutionContext());
    } finally {
      command.terminate();
      server.stop(0);
    }
    return bodies.stream().flatMap(String::lines).filter(l -> !l.isEmpty()).toList();
  }

  private static final String SUCCESS = "{\"text\":\"Success\",\"code\":0}";
  private static final String FORBIDDEN = "{\"text\":\"Invalid token\",\"code\":4}";
  private static final String INVALID_DATA = "{\"text\":\"Invalid data format\",\"code\":6}";
  private static final RecordFleakData EVENT_A =
      new RecordFleakData(Map.of("seq", new StringPrimitiveFleakData("a")));
  private static final RecordFleakData EVENT_B =
      new RecordFleakData(Map.of("seq", new StringPrimitiveFleakData("b")));
  private static final List<String> LINES_AB =
      List.of("{\"event\":{\"seq\":\"a\"}}", "{\"event\":{\"seq\":\"b\"}}");
  private static final List<String> STORE_METRICS =
      List.of(
          "store_forward_buffered_count",
          "store_forward_replayed_count",
          "store_forward_dropped_count",
          "sink_output_count",
          "sink_error_count");

  @TempDir Path tempDir;

  @Test
  void config_withoutStoreFields_usesStoreDefaults() {
    SplunkHecSinkDto.Config expected =
        SplunkHecSinkDto.Config.builder().hecUrl("https://splunk:8088").credentialId("c").build();
    expected.setStoreAndForwardEnabled(false);
    expected.setLocalStoreMaxBytes(1_073_741_824L);
    expected.setForwardRetryIntervalMillis(5_000L);

    SplunkHecSinkDto.Config actual =
        OBJECT_MAPPER.convertValue(
            Map.of("hecUrl", "https://splunk:8088", "credentialId", "c"),
            SplunkHecSinkDto.Config.class);

    assertEquals(expected, actual);
  }

  @Test
  void storeOn_forbidden_storesBatchAndDeliversItOnceHecAccepts() throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    try (StubHec hec = new StubHec()) {
      hec.answer(403, FORBIDDEN);
      SplunkHecSinkCommand command = command(hec, jobContext(false), true, null, metrics);
      SinkResult result;
      try {
        result = write(command);
        hec.answer(200, SUCCESS);
        await(() -> !storeForward(command).isBuffering());
      } finally {
        command.terminate();
      }

      assertEquals(new SinkResult(2, 0, List.of()), result);
      assertEquals(LINES_AB, hec.accepted());
      assertEquals(totals(2, 2, 0, 2, 0), metrics.totals(STORE_METRICS));
    }
  }

  @Test
  void storeOn_recordProblem_isCountedAndNotStored() throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    try (StubHec hec = new StubHec()) {
      hec.answer(400, INVALID_DATA);
      SplunkHecSinkCommand command = command(hec, jobContext(false), true, null, metrics);
      SinkResult result;
      boolean buffering;
      try {
        result = write(command);
        buffering = storeForward(command).isBuffering();
      } finally {
        command.terminate();
      }

      String reason = "Splunk HEC error 400: Invalid data format";
      assertEquals(
          new SinkResult(
              2, 0, List.of(new ErrorOutput(EVENT_A, reason), new ErrorOutput(EVENT_B, reason))),
          result);
      assertFalse(buffering);
      assertEquals(totals(0, 0, 0, 0, 2), metrics.totals(STORE_METRICS));
    }
  }

  @Test
  void storeOn_recordProblemOnReplay_isCountedAsSinkErrorNotDropped() throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    try (StubHec hec = new StubHec()) {
      hec.answer(403, FORBIDDEN);
      SplunkHecSinkCommand command = command(hec, jobContext(false), true, null, metrics);
      try {
        write(command);
        hec.answer(400, INVALID_DATA);
        await(() -> !storeForward(command).isBuffering());
      } finally {
        command.terminate();
      }

      assertEquals(List.of(), hec.accepted());
      assertEquals(totals(2, 2, 0, 0, 2), metrics.totals(STORE_METRICS));
    }
  }

  @Test
  void storedRecords_areDeliveredByTheSamePipelineAfterRestart() throws Exception {
    CapturingMetrics firstRun = new CapturingMetrics();
    CapturingMetrics secondRun = new CapturingMetrics();
    try (StubHec hec = new StubHec()) {
      hec.answer(403, FORBIDDEN);
      SplunkHecSinkCommand first = command(hec, jobContext(false), true, null, firstRun);
      try {
        write(first);
      } finally {
        first.terminate();
      }

      hec.answer(200, SUCCESS);
      SplunkHecSinkCommand second = command(hec, jobContext(false), true, null, secondRun);
      try {
        await(() -> !storeForward(second).isBuffering());
      } finally {
        second.terminate();
      }

      assertEquals(LINES_AB, hec.accepted());
      assertEquals(totals(2, 0, 0, 0, 0), firstRun.totals(STORE_METRICS));
      assertEquals(totals(0, 2, 0, 2, 0), secondRun.totals(STORE_METRICS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void boundedOrTestMode_withStoreEnabled_createsNoStore(boolean bounded) throws Exception {
    try (StubHec hec = new StubHec()) {
      ExecutionHooks hooks =
          bounded
              ? new ExecutionHooks(
                  () -> {},
                  new ExecutionHooks.Effects() {
                    public long started() {
                      return 1;
                    }

                    public void finished(long id, EffectOutcome outcome) {}
                  },
                  Runnable::run)
              : null;
      SplunkHecSinkCommand command =
          command(
              hec,
              jobContext(!bounded),
              true,
              hooks,
              new MetricClientProvider.NoopMetricClientProvider());
      SinkStoreForward store;
      try {
        write(command);
        store = storeForward(command);
      } finally {
        command.terminate();
      }

      assertSame(SinkStoreForward.noop(), store);
      assertFalse(Files.exists(storeBase()));
      assertEquals(LINES_AB, hec.accepted());
    }
  }

  @Test
  void storeOff_forbidden_isCountedAsToday() throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    try (StubHec hec = new StubHec()) {
      hec.answer(403, FORBIDDEN);
      SplunkHecSinkCommand command = command(hec, jobContext(false), false, null, metrics);
      SinkResult result;
      SinkStoreForward store;
      try {
        result = write(command);
        store = storeForward(command);
      } finally {
        command.terminate();
      }

      String reason = "Splunk HEC error 403: Invalid token";
      assertEquals(
          new SinkResult(
              2, 0, List.of(new ErrorOutput(EVENT_A, reason), new ErrorOutput(EVENT_B, reason))),
          result);
      assertSame(SinkStoreForward.noop(), store);
      assertFalse(Files.exists(storeBase()));
      assertEquals(totals(0, 0, 0, 0, 2), metrics.totals(STORE_METRICS));
    }
  }

  @Test
  void storeOn_failureAfterTheStoreIsOpened_releasesTheStoreDir() throws Exception {
    try (StubHec hec = new StubHec()) {
      IllegalStateException initFailure = new IllegalStateException("flusher init failed");
      try (MockedConstruction<SplunkHecSinkFlusher> ignored =
          mockConstruction(
              SplunkHecSinkFlusher.class,
              (flusher, context) -> doThrow(initFailure).when(flusher).initialize())) {
        IllegalStateException thrown =
            assertThrows(
                IllegalStateException.class,
                () -> command(hec, jobContext(false), true, null, new CapturingMetrics()));
        assertSame(initFailure, thrown);
      }

      command(hec, jobContext(false), true, null, new CapturingMetrics()).terminate();
    }
  }

  private Path storeBase() {
    return tempDir.resolve("store");
  }

  private static JobContext jobContext(boolean testMode) {
    HashMap<String, Serializable> properties = new HashMap<>();
    properties.put("hec_token", new HashMap<>(Map.of("key", "abcd-1234")));
    if (testMode) {
      properties.put(JobContext.FLAG_TEST_MODE, true);
    }
    return JobContext.builder()
        .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
        .otherProperties(properties)
        .build();
  }

  private SplunkHecSinkCommand command(
      StubHec hec,
      JobContext jobContext,
      boolean storeAndForward,
      ExecutionHooks hooks,
      MetricClientProvider metrics) {
    Map<String, Object> nodeConfig = new HashMap<>();
    nodeConfig.put("hecUrl", hec.url());
    nodeConfig.put("credentialId", "hec_token");
    nodeConfig.put("batchSize", 2);
    nodeConfig.put("storeAndForwardEnabled", storeAndForward);
    nodeConfig.put("localStorePath", storeBase().toString());
    nodeConfig.put("forwardRetryIntervalMillis", 20);
    SplunkHecSinkCommand command =
        (SplunkHecSinkCommand)
            new SplunkHecSinkCommandFactory().createCommand("splunkNode", jobContext);
    command.parseAndValidateArg(nodeConfig);
    if (hooks != null) {
      command.setExecutionHooks(hooks);
    }
    command.initialize(metrics);
    return command;
  }

  private static SinkResult write(SplunkHecSinkCommand command) {
    return command.writeToSink(
        List.of(EVENT_A, EVENT_B), "test_user", command.getExecutionContext());
  }

  private static SinkStoreForward storeForward(SplunkHecSinkCommand command) {
    return ((SinkExecutionContext<?>) command.getExecutionContext()).storeForward();
  }

  private static Map<String, Long> totals(
      long buffered, long replayed, long dropped, long sinkOutput, long sinkError) {
    List<Long> values = List.of(buffered, replayed, dropped, sinkOutput, sinkError);
    Map<String, Long> totals = new LinkedHashMap<>();
    for (int i = 0; i < STORE_METRICS.size(); i++) {
      totals.put(STORE_METRICS.get(i), values.get(i));
    }
    return totals;
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail("condition not met within 10s");
      }
      Thread.sleep(10);
    }
  }

  private static final class StubHec implements AutoCloseable {
    private final HttpServer server;
    private final List<String> accepted = Collections.synchronizedList(new ArrayList<>());
    private volatile int status = 200;
    private volatile String body = SUCCESS;

    StubHec() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/services/collector/event",
          exchange -> {
            String request =
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            int replyStatus = status;
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            if (replyStatus == 200) {
              Arrays.stream(request.split("\n")).filter(l -> !l.isEmpty()).forEach(accepted::add);
            }
            exchange.sendResponseHeaders(replyStatus, out.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(out);
            }
          });
      server.start();
    }

    synchronized void answer(int status, String body) {
      this.body = body;
      this.status = status;
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event";
    }

    List<String> accepted() {
      synchronized (accepted) {
        return List.copyOf(accepted);
      }
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static final class CapturingMetrics
      extends MetricClientProvider.NoopMetricClientProvider {
    private final Map<String, AtomicLong> totals = new ConcurrentHashMap<>();

    Map<String, Long> totals(List<String> names) {
      Map<String, Long> result = new LinkedHashMap<>();
      for (String name : names) {
        result.put(name, totals.getOrDefault(name, new AtomicLong()).get());
      }
      return result;
    }

    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      AtomicLong total = totals.computeIfAbsent(name, k -> new AtomicLong());
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
  }
}
