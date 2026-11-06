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

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_INDEX;
import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SplunkHecSinkFlusherTest {

  private static final String SUCCESS = "{\"text\":\"Success\",\"code\":0}";

  private FakeHec hec;
  private ChronicleStoreForward store;

  @TempDir Path storeDir;

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.close();
    }
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
  void boundedAcceptedThenDisconnectedDoesNotRetryOrRecoverIndividualRecords() throws Exception {
    hec = FakeHec.replying(lines -> Reply.NO_RESPONSE);
    var hooks = boundedHooks();
    try (var sink = flusher(100, JobContext.builder().executionHooks(hooks).build())) {
      assertThrows(IOException.class, () -> sink.flushBounded(events("a", "b"), Map.of(), hooks));
      sink.abort();
    }
    assertEquals(List.of(2), hec.requestSizes());
    assertTrue(dlqWriter.deadLetters.isEmpty());
  }

  @Test
  void boundedServerErrorDoesNotEnterNormalRetryLoop() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(503, 9, "unavailable"));
    var hooks = boundedHooks();
    try (var sink = flusher(100, JobContext.builder().executionHooks(hooks).build())) {
      var result = sink.flushBounded(events("a", "b"), Map.of(), hooks);
      assertEquals(2L, result.effectOutcome().unknownCount());
      assertEquals(0L, result.effectOutcome().definiteFailureCount());
    }
    assertEquals(List.of(2), hec.requestSizes());
    assertTrue(dlqWriter.deadLetters.isEmpty());
  }

  @Test
  void boundedExplicitRejectionPreservesNegativeReceiptWithoutLoggingResponse() throws Exception {
    String secret = "private-hec-credential-for-regression";
    hec = FakeHec.replying(lines -> Reply.hec(403, 4, secret));
    var logger =
        (org.apache.logging.log4j.core.Logger)
            org.apache.logging.log4j.LogManager.getLogger(SplunkHecSinkFlusher.class);
    var logs = new ArrayList<org.apache.logging.log4j.core.LogEvent>();
    var appender =
        new org.apache.logging.log4j.core.appender.AbstractAppender(
            "bounded-hec",
            null,
            org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
            false,
            org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
          public void append(org.apache.logging.log4j.core.LogEvent event) {
            logs.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    var hooks = boundedHooks();
    try (var sink = flusher(100, JobContext.builder().executionHooks(hooks).build())) {
      var result = sink.flushBounded(events("a", "b"), Map.of(), hooks);
      var outcome = result.effectOutcome();
      assertEquals(2L, outcome.attemptedCount());
      assertEquals(2L, outcome.definiteFailureCount());
      assertEquals(0L, outcome.unknownCount());
      assertEquals(0L, outcome.acknowledgedCount());
      assertEquals(
          io.fleak.zephflow.api.execution.EffectOutcome.Delivery.FAILED, outcome.delivery());
      assertEquals(2, result.errorOutputList().size());
      assertTrue(result.errorOutputList().getFirst().errorMessage().contains(secret));
      assertTrue(
          logs.stream()
              .noneMatch(event -> event.getMessage().getFormattedMessage().contains(secret)));
      assertTrue(logs.stream().allMatch(event -> event.getThrown() == null));
      assertEquals(List.of(2), hec.requestSizes());
      assertTrue(dlqWriter.deadLetters.isEmpty());
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  private static io.fleak.zephflow.api.execution.ExecutionHooks boundedHooks() {
    return new io.fleak.zephflow.api.execution.ExecutionHooks(
        () -> {},
        new io.fleak.zephflow.api.execution.ExecutionHooks.Effects() {
          public long started() {
            return 1;
          }

          public void finished(long id, io.fleak.zephflow.api.execution.EffectOutcome outcome) {}
        },
        Runnable::run);
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
  void ioFailure_timer_countsFailedSliceWithoutDeadLettering() throws Exception {
    hec = FakeHec.replying(lines -> Reply.NO_RESPONSE);
    SplunkHecSinkFlusher flusher = flusher(3);
    flusher.flush(events("c", "bad"), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(List.of(2, 2, 2), hec.requestSizes());
    assertEquals(0, sinkOutputCounter.total.get());
    assertEquals(2, sinkErrorCounter.total.get());
    assertTrue(dlqWriter.deadLetters.isEmpty());
  }

  @Test
  void timerFailure_countsEachFailedRecordWithoutDeadLettering() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(401, 2, "Token is required"));
    SplunkHecSinkFlusher flusher = flusher(500);
    flusher.flush(events("a", "b"), Map.of());

    flusher.executeScheduledFlush();

    assertTrue(dlqWriter.deadLetters.isEmpty());
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

  private final RecordingCounter buffered = new RecordingCounter();
  private final RecordingCounter replayed = new RecordingCounter();
  private final RecordingCounter dropped = new RecordingCounter();
  private final RecordingCounter preprocessErrors = new RecordingCounter();

  private record Counts(
      long buffered, long replayed, long dropped, long sinkOutput, long sinkError) {}

  private Counts counts() {
    return new Counts(
        buffered.total.get(),
        replayed.total.get(),
        dropped.total.get(),
        sinkOutputCounter.total.get(),
        sinkErrorCounter.total.get());
  }

  private ChronicleStoreForward enableStore(SplunkHecSinkFlusher flusher) {
    HecDestinationFailureClassifier classifier = new HecDestinationFailureClassifier();
    store =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(storeDir, 1 << 20, 20, 2, "splunkNode"),
            classifier::isDestinationFailure,
            buffered,
            replayed,
            dropped);
    flusher.enableStoreForward(store, classifier);
    return store;
  }

  private void startReplay(SplunkHecSinkFlusher flusher) {
    store.start(
        flusher.replayTarget(
            new SplunkHecSinkMessageProcessor(null, null, null),
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            preprocessErrors));
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

  private static List<ErrorOutput> errors(
      SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events, String message) {
    return events.rawAndPreparedList().stream()
        .map(pair -> new ErrorOutput(pair.getLeft(), message))
        .toList();
  }

  @Test
  void forbidden_withStore_storesBatchAndReplayDeliversItOnceHecAccepts() throws Exception {
    AtomicReference<Reply> reply = new AtomicReference<>(Reply.hec(403, 4, "Invalid token"));
    hec = FakeHec.replying(lines -> reply.get());
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);
    startReplay(flusher);

    SimpleSinkCommand.FlushResult result = flusher.flush(events("a", "b"), Map.of());

    assertEquals(new SimpleSinkCommand.FlushResult(0, 0, List.of()), result);
    assertTrue(store.isBuffering());
    reply.set(Reply.ok());
    await(() -> !store.isBuffering());
    assertEquals(new Counts(2, 2, 0, 2, 0), counts());
    assertEquals(
        List.of("{\"event\":{\"msg\":\"a\"}}", "{\"event\":{\"msg\":\"b\"}}"),
        hec.requests().getLast());
  }

  @Test
  void timerFlush_destinationFailure_withStore_storesInsteadOfCountingLost() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(401, 2, "Token is required"));
    SplunkHecSinkFlusher flusher = flusher(500);
    enableStore(flusher);
    flusher.flush(events("a", "b"), Map.of());

    flusher.executeScheduledFlush();

    assertTrue(store.isBuffering());
    assertEquals(new Counts(2, 0, 0, 0, 0), counts());
  }

  @Test
  void destinationFailureOnFirstSlice_storesLaterSlicesWithoutSending() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(403, 4, "Invalid token"));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);

    SimpleSinkCommand.FlushResult result = flusher.flush(events("a", "b", "c", "d"), Map.of());

    assertEquals(new SimpleSinkCommand.FlushResult(0, 0, List.of()), result);
    assertEquals(List.of(2), hec.requestSizes());
    assertEquals(new Counts(4, 0, 0, 0, 0), counts());
  }

  @Test
  void recordProblem_withStore_isFailedAndNotStored() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(400, 6, "Invalid data format"));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events = events("a", "b");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, errors(events, "Splunk HEC error 400: Invalid data format")),
        result);
    assertFalse(store.isBuffering());
    assertEquals(new Counts(0, 0, 0, 0, 0), counts());
  }

  private static Reply incorrectIndex(Integer invalidEvent) {
    String eventNumber = invalidEvent == null ? "" : ",\"invalid-event-number\":" + invalidEvent;
    return new Reply(400, "{\"text\":\"Incorrect index\",\"code\":7" + eventNumber + "}");
  }

  private static SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> withIndexes(
      String... indexes) {
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    for (int i = 0; i < indexes.length; i++) {
      Map<String, FleakData> payload = new HashMap<>();
      payload.put("msg", new StringPrimitiveFleakData("e" + i));
      if (indexes[i] != null) {
        payload.put(FIELD_NAME_INDEX, new StringPrimitiveFleakData(indexes[i]));
      }
      String line = "{\"event\":{\"msg\":\"e" + i + "\"}}\n";
      events.add(
          new RecordFleakData(payload),
          new SplunkHecOutboundEvent(line.getBytes(StandardCharsets.UTF_8)));
    }
    return events;
  }

  @Test
  void refusedRecordIndex_withStore_isFailedAndNotStored() throws Exception {
    hec = FakeHec.replying(lines -> incorrectIndex(2));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        withIndexes(null, "mian");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, errors(events, "Splunk HEC error 400: Incorrect index")),
        result);
    assertFalse(store.isBuffering());
    assertEquals(new Counts(0, 0, 0, 0, 0), counts());
  }

  @Test
  void refusedConfiguredIndex_withStore_isStored() throws Exception {
    hec = FakeHec.replying(lines -> incorrectIndex(1));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);

    SimpleSinkCommand.FlushResult result = flusher.flush(withIndexes(null, "main"), Map.of());

    assertEquals(new SimpleSinkCommand.FlushResult(0, 0, List.of()), result);
    assertTrue(store.isBuffering());
    assertEquals(new Counts(2, 0, 0, 0, 0), counts());
  }

  @Test
  void refusedIndexWithoutEventNumber_isRecordProblemWhenAnyRecordSetsIndex() throws Exception {
    hec = FakeHec.replying(lines -> incorrectIndex(null));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        withIndexes(null, "mian");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, errors(events, "Splunk HEC error 400: Incorrect index")),
        result);
    assertFalse(store.isBuffering());
  }

  @Test
  void refusedRecordIndexOnReplay_isDroppedFromTheStore() throws Exception {
    hec = FakeHec.replying(lines -> incorrectIndex(1));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);
    startReplay(flusher);

    store.offer(
        withIndexes("mian", "mian").rawAndPreparedList().stream()
            .map(pair -> pair.getLeft())
            .toList());

    await(() -> !store.isBuffering());
    assertEquals(new Counts(2, 2, 0, 0, 2), counts());
    assertEquals(List.of(2), hec.requestSizes());
  }

  @Test
  void recordProblemOnReplay_isCountedAsSinkErrorNotDropped() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(400, 6, "Invalid data format"));
    SplunkHecSinkFlusher flusher = flusher(2);
    enableStore(flusher);
    startReplay(flusher);

    store.offer(
        events("a", "b").rawAndPreparedList().stream().map(pair -> pair.getLeft()).toList());

    await(() -> !store.isBuffering());
    assertEquals(new Counts(2, 2, 0, 0, 2), counts());
    assertEquals(List.of(2), hec.requestSizes());
  }

  @Test
  void forbidden_withNoopStore_failsBatchAsToday() throws Exception {
    hec = FakeHec.replying(lines -> Reply.hec(403, 4, "Invalid token"));
    SplunkHecSinkFlusher flusher = flusher(2);
    flusher.enableStoreForward(SinkStoreForward.noop(), new HecDestinationFailureClassifier());
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events = events("a", "b");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, errors(events, "Splunk HEC error 403: Invalid token")),
        result);
  }

  @Test
  void flushNow_throwsOnDestinationFailureAndFailsRecordProblems() throws Exception {
    AtomicReference<Reply> reply = new AtomicReference<>(Reply.hec(403, 4, "Invalid token"));
    hec = FakeHec.replying(lines -> reply.get());
    SplunkHecSinkFlusher flusher = flusher(2);
    flusher.enableStoreForward(SinkStoreForward.noop(), new HecDestinationFailureClassifier());
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events = events("a", "b");

    SplunkHecSinkFlusher.HecResponseException thrown =
        assertThrows(
            SplunkHecSinkFlusher.HecResponseException.class,
            () -> flusher.flushNow(events, Map.of()));
    reply.set(Reply.hec(400, 6, "Invalid data format"));
    SimpleSinkCommand.FlushResult result = flusher.flushNow(events, Map.of());

    assertEquals(403, thrown.getStatus());
    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, errors(events, "Splunk HEC error 400: Invalid data format")),
        result);
  }
}
