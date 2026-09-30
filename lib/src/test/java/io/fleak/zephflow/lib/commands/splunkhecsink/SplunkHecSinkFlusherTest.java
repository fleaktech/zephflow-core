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

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SplunkHecSinkFlusherTest {

  private static final String SUCCESS = "{\"text\":\"Success\",\"code\":0}";

  private FakeHec hec;

  @AfterEach
  void tearDown() {
    if (hec != null) {
      hec.close();
    }
    Thread.interrupted();
  }

  private record Reply(int status, String body) {
    static final Reply NO_RESPONSE = new Reply(-1, null);

    static Reply ok() {
      return new Reply(200, SUCCESS);
    }

    static Reply hec(int status, int code, String text) {
      return new Reply(status, "{\"text\":\"" + text + "\",\"code\":" + code + "}");
    }
  }

  private record FakeHec(
      HttpServer server, List<List<String>> requests, CountDownLatch firstRequest)
      implements AutoCloseable {

    static FakeHec replying(Function<List<String>, Reply> handler) throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      List<List<String>> requests = Collections.synchronizedList(new ArrayList<>());
      CountDownLatch firstRequest = new CountDownLatch(1);
      server.createContext(
          "/services/collector/event",
          exchange -> {
            String body =
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            List<String> lines = Arrays.stream(body.split("\n")).filter(l -> !l.isEmpty()).toList();
            requests.add(lines);
            firstRequest.countDown();
            respond(exchange, handler.apply(lines));
          });
      server.start();
      return new FakeHec(server, requests, firstRequest);
    }

    private static void respond(HttpExchange exchange, Reply reply) throws IOException {
      if (reply == Reply.NO_RESPONSE) {
        exchange.close();
        return;
      }
      byte[] out = reply.body().getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(reply.status(), out.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(out);
      }
    }

    String url() {
      return "http://127.0.0.1:" + server.getAddress().getPort() + "/services/collector/event";
    }

    List<Integer> requestSizes() {
      synchronized (requests) {
        return requests.stream().map(List::size).toList();
      }
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static class RecordingCounter implements FleakCounter {
    final AtomicLong total = new AtomicLong();

    @Override
    public void increase(Map<String, String> additionalTags) {
      total.incrementAndGet();
    }

    @Override
    public void increase(long n, Map<String, String> additionalTags) {
      total.addAndGet(n);
    }
  }

  private static class RecordingDlqWriter extends DlqWriter {
    final List<DeadLetter> deadLetters = Collections.synchronizedList(new ArrayList<>());

    @Override
    protected void doWrite(DeadLetter deadLetter) {
      deadLetters.add(deadLetter);
    }

    @Override
    public void open() {}

    @Override
    public void close() {}
  }

  private final RecordingCounter sinkOutputCounter = new RecordingCounter();
  private final RecordingCounter outputSizeCounter = new RecordingCounter();
  private final RecordingCounter sinkErrorCounter = new RecordingCounter();
  private final RecordingDlqWriter dlqWriter = new RecordingDlqWriter();

  private SplunkHecSinkFlusher flusher(int batchSize) {
    return flusher(batchSize, null);
  }

  private SplunkHecSinkFlusher flusher(int batchSize, JobContext jobContext) {
    return new SplunkHecSinkFlusher(
        hec.url(),
        "abcd-1234",
        HttpClient.newHttpClient(),
        batchSize,
        SplunkHecSinkCommand.FLUSH_INTERVAL_MS,
        dlqWriter,
        jobContext,
        "splunkNode",
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter);
  }

  private static SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events(
      String... names) {
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    for (String name : names) {
      RecordFleakData raw = new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData(name)));
      String line = "{\"event\":{\"msg\":\"" + name + "\"}}\n";
      events.add(raw, new SplunkHecOutboundEvent(line.getBytes(StandardCharsets.UTF_8)));
    }
    return events;
  }

  private static SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> numbered(int n) {
    return events(
        java.util.stream.IntStream.range(0, n).mapToObj(i -> "e" + i).toArray(String[]::new));
  }

  private static boolean contains(List<String> lines, String name) {
    return lines.stream().anyMatch(l -> l.contains("\"" + name + "\""));
  }

  private static String failedMsg(SimpleSinkCommand.FlushResult result, int index) {
    return result.errorOutputList().get(index).errorMessage();
  }

  @Test
  void flush_belowBatchSize_buffersWithoutSending() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    SplunkHecSinkFlusher flusher = flusher(3);

    SimpleSinkCommand.FlushResult first = flusher.flush(events("a"), Map.of());
    SimpleSinkCommand.FlushResult second = flusher.flush(events("b"), Map.of());

    assertEquals(0, first.successCount());
    assertEquals(0, second.successCount());
    assertTrue(hec.requests().isEmpty());
  }

  @Test
  void flush_reachingBatchSize_sendsOneRequestWithAllEventsInOrder() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    SplunkHecSinkFlusher flusher = flusher(3);

    flusher.flush(events("a"), Map.of());
    flusher.flush(events("b"), Map.of());
    SimpleSinkCommand.FlushResult result = flusher.flush(events("c"), Map.of());

    assertEquals(3, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertEquals(1, hec.requests().size());
    List<String> lines = hec.requests().getFirst();
    assertEquals(
        List.of(
            "{\"event\":{\"msg\":\"a\"}}",
            "{\"event\":{\"msg\":\"b\"}}",
            "{\"event\":{\"msg\":\"c\"}}"),
        lines);
  }

  @Test
  void flush_callLargerThanBatchSize_isSlicedIntoRequestsOfBatchSize() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    SplunkHecSinkFlusher flusher = flusher(500);

    SimpleSinkCommand.FlushResult result = flusher.flush(numbered(1250), Map.of());

    assertEquals(1250, result.successCount());
    assertEquals(List.of(500, 500, 250), hec.requestSizes());
  }

  @Test
  void flush_sendsSplunkAuthHeaderAndJsonContentType() throws Exception {
    Map<String, String> seenHeaders = new HashMap<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/services/collector/event",
        exchange -> {
          seenHeaders.put("Authorization", exchange.getRequestHeaders().getFirst("Authorization"));
          seenHeaders.put("Content-Type", exchange.getRequestHeaders().getFirst("Content-Type"));
          exchange.getRequestBody().readAllBytes();
          FakeHec.respond(exchange, Reply.ok());
        });
    server.start();
    hec = new FakeHec(server, new ArrayList<>(), new CountDownLatch(0));

    flusher(1).flush(events("a"), Map.of());

    assertEquals("Splunk abcd-1234", seenHeaders.get("Authorization"));
    assertEquals("application/json", seenHeaders.get("Content-Type"));
  }

  @Test
  void executeScheduledFlush_sendsPartialBufferAndReportsMetrics() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    SplunkHecSinkFlusher flusher = flusher(500);
    flusher.flush(events("a", "b"), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(List.of(2), hec.requestSizes());
    assertEquals(2, sinkOutputCounter.total.get());
    assertTrue(outputSizeCounter.total.get() > 0);
    assertEquals(0, sinkErrorCounter.total.get());
  }

  @Test
  void syncMode_sendsOnEveryCall() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    JobContext testRun =
        JobContext.builder()
            .otherProperties(new HashMap<>(Map.of(JobContext.FLAG_TEST_MODE, true)))
            .build();
    SplunkHecSinkFlusher flusher = flusher(500, testRun);

    SimpleSinkCommand.FlushResult first = flusher.flush(events("a"), Map.of());
    SimpleSinkCommand.FlushResult second = flusher.flush(events("b"), Map.of());

    assertEquals(1, first.successCount());
    assertEquals(1, second.successCount());
    assertEquals(List.of(1, 1), hec.requestSizes());
  }

  @Test
  void close_flushesRemainder() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    SplunkHecSinkFlusher flusher = flusher(500);
    flusher.flush(events("a", "b", "c"), Map.of());

    flusher.close();

    assertEquals(List.of(3), hec.requestSizes());
    assertEquals(3, sinkOutputCounter.total.get());
  }

  @Test
  void serverError_retriedThreeTimesThenFailsSlice() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(503, 9, "Server is busy"));

    SimpleSinkCommand.FlushResult result = flusher(2).flush(events("a", "b"), Map.of());

    assertEquals(3, hec.requests().size());
    assertEquals(0, result.successCount());
    assertEquals(2, result.errorOutputList().size());
    assertEquals("Splunk HEC error 503: Server is busy", failedMsg(result, 0));
  }

  @Test
  void tooManyRequests_isRetried() throws Exception {
    AtomicBoolean first = new AtomicBoolean(true);
    hec =
        FakeHec.replying(
            lines ->
                first.getAndSet(false)
                    ? Reply.hec(429, 26, "HEC queue is at capacity")
                    : Reply.ok());

    SimpleSinkCommand.FlushResult result = flusher(1).flush(events("a"), Map.of());

    assertEquals(2, hec.requests().size());
    assertEquals(1, result.successCount());
  }

  @Test
  void tooManyRequests_retriedThreeTimesThenFailsSlice() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(429, 26, "HEC queue is at capacity"));

    SimpleSinkCommand.FlushResult result = flusher(2).flush(events("a", "b"), Map.of());

    assertEquals(3, hec.requests().size());
    assertEquals(0, result.successCount());
    assertEquals(2, result.errorOutputList().size());
    assertEquals("Splunk HEC error 429: HEC queue is at capacity", failedMsg(result, 0));
  }

  @Test
  void ioException_isRetried() throws Exception {
    AtomicBoolean first = new AtomicBoolean(true);
    hec = FakeHec.replying(lines -> first.getAndSet(false) ? Reply.NO_RESPONSE : Reply.ok());

    SimpleSinkCommand.FlushResult result = flusher(1).flush(events("a"), Map.of());

    assertEquals(2, hec.requests().size());
    assertEquals(1, result.successCount());
  }

  @Test
  void unauthorized_notRetriedAndFailsWholeSlice() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(401, 2, "Token is required"));

    SimpleSinkCommand.FlushResult result = flusher(3).flush(events("a", "b", "c"), Map.of());

    assertEquals(1, hec.requests().size());
    assertEquals(3, result.errorOutputList().size());
    assertEquals("Splunk HEC error 401: Token is required", failedMsg(result, 0));
  }

  @Test
  void forbidden_notRetriedAndFailsWholeSlice() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(403, 4, "Invalid token"));

    SimpleSinkCommand.FlushResult result = flusher(2).flush(events("a", "b"), Map.of());

    assertEquals(1, hec.requests().size());
    assertEquals(2, result.errorOutputList().size());
  }

  @Test
  void badRequest_notRetriedAndFailsWholeSlice() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(400, 6, "Invalid data format"));

    SimpleSinkCommand.FlushResult result = flusher(3).flush(events("a", "b", "c"), Map.of());

    assertEquals(1, hec.requests().size());
    assertEquals(0, result.successCount());
    assertEquals(3, result.errorOutputList().size());
    assertEquals("Splunk HEC error 400: Invalid data format", failedMsg(result, 0));
  }

  @Test
  void badRequestWithNonJsonBody_usesRawBodyAsReason() throws Exception {
    hec = FakeHec.replying(lines -> new Reply(400, "bad request"));

    SimpleSinkCommand.FlushResult result = flusher(2).flush(events("a", "b"), Map.of());

    assertEquals(1, hec.requests().size());
    assertEquals(2, result.errorOutputList().size());
    assertEquals("Splunk HEC error 400: bad request", failedMsg(result, 0));
  }

  @Test
  void failedSlice_doesNotAffectOtherSlices() throws Exception {
    hec =
        FakeHec.replying(
            lines -> contains(lines, "bad") ? Reply.hec(401, 2, "Token is required") : Reply.ok());

    SimpleSinkCommand.FlushResult result = flusher(2).flush(events("a", "b", "c", "bad"), Map.of());

    assertEquals(List.of(2, 2), hec.requestSizes());
    assertEquals(2, result.successCount());
    assertEquals(2, result.errorOutputList().size());
  }

  @Test
  void ioFailureOnOneSlice_isContained_inBand() throws Exception {
    hec = FakeHec.replying(lines -> contains(lines, "bad") ? Reply.NO_RESPONSE : Reply.ok());

    SimpleSinkCommand.FlushResult result = flusher(2).flush(events("a", "b", "c", "bad"), Map.of());

    assertEquals(List.of(2, 2, 2, 2), hec.requestSizes());
    assertEquals(2, result.successCount());
    assertEquals(2, result.errorOutputList().size());
  }

  @Test
  void ioFailure_timer_writesFailedSliceToDlqOnce() throws Exception {
    hec = FakeHec.replying(lines -> Reply.NO_RESPONSE);
    SplunkHecSinkFlusher flusher = flusher(3);
    flusher.flush(events("c", "bad"), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(List.of(2, 2, 2), hec.requestSizes());
    assertEquals(0, sinkOutputCounter.total.get());
    assertEquals(2, sinkErrorCounter.total.get());
    assertEquals(2, dlqWriter.deadLetters.size());
  }

  @Test
  void timerFailure_writesEachFailedRecordToDlqOnce() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(401, 2, "Token is required"));
    SplunkHecSinkFlusher flusher = flusher(500);
    flusher.flush(events("a", "b"), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(2, dlqWriter.deadLetters.size());
    assertEquals(
        "Splunk HEC error 401: Token is required",
        dlqWriter.deadLetters.getFirst().getErrorMessage().toString());
    assertEquals(2, sinkErrorCounter.total.get());
    assertEquals(0, sinkOutputCounter.total.get());
  }

  @Test
  void interruptedThread_sendsNothing() throws Exception {
    hec = FakeHec.replying(lines -> Reply.ok());
    SplunkHecSinkFlusher flusher = flusher(1);

    Thread.currentThread().interrupt();
    SimpleSinkCommand.FlushResult result = flusher.flush(events("a"), Map.of());
    boolean stillInterrupted = Thread.interrupted();

    assertTrue(stillInterrupted);
    assertTrue(hec.requests().isEmpty());
    assertEquals(1, result.errorOutputList().size());
  }

  @Test
  void interruptDuringBackoff_stopsRetryingAndKeepsFlag() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(503, 9, "Server is busy"));
    SplunkHecSinkFlusher flusher = flusher(1);
    List<SimpleSinkCommand.FlushResult> results = Collections.synchronizedList(new ArrayList<>());
    AtomicBoolean interruptedAfter = new AtomicBoolean();
    Thread worker =
        new Thread(
            () -> {
              try {
                results.add(flusher.flush(events("a"), Map.of()));
              } catch (Exception e) {
                fail(e);
              }
              interruptedAfter.set(Thread.currentThread().isInterrupted());
            });

    worker.start();
    assertTrue(hec.firstRequest().await(10, TimeUnit.SECONDS));
    Thread.sleep(200);
    worker.interrupt();
    worker.join(10_000);

    assertFalse(worker.isAlive());
    assertEquals(1, hec.requests().size());
    assertTrue(interruptedAfter.get());
    assertEquals(1, results.getFirst().errorOutputList().size());
  }
}
