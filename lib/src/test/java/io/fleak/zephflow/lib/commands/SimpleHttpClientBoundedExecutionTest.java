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
package io.fleak.zephflow.lib.commands;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.lib.utils.SecurityUtils;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.http.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(20)
class SimpleHttpClientBoundedExecutionTest {
  private static final String SECRET = "private-provider-token";
  private static final String ROW = "private-request-record";
  private static final String URL = "https://203.0.113.1/ingest?token=" + SECRET;
  private static final List<String> HEADERS = List.of("Authorization: Bearer " + SECRET);

  @ParameterizedTest
  @ValueSource(strings = {"denied", "uri", "header"})
  void boundedBytesLocalRejectionNeverAdmitsAnAttemptAndDoesNotExposeItsCause(String reason) {
    HttpClient http = mock();
    var client = new SimpleHttpClient(3, 0, http, new LimitedSizeBodyHandler(1024));
    String endpoint = reason.equals("uri") ? "https://invalid host/" + SECRET : URL;
    List<String> headers =
        reason.equals("header") ? List.of("Authorization: " + SECRET + "\ninvalid") : HEADERS;
    Runnable admission = mock();
    try (var security = mockStatic(SecurityUtils.class);
        var logs = new Logs()) {
      security
          .when(() -> SecurityUtils.isUrlAllowed(endpoint))
          .thenReturn(!reason.equals("denied"));
      var rejected =
          assertThrows(
              SimpleHttpClient.LocalRequestRejectedException.class,
              () ->
                  client.sendHttpBytes(
                      endpoint,
                      SimpleHttpClient.HttpMethodType.POST,
                      new byte[] {1},
                      headers,
                      HttpClient.Version.HTTP_2,
                      hooks(() -> {}),
                      admission));
      assertNull(rejected.getCause());
      assertFalse(rejected.toString().contains(SECRET));
      assertFalse(logs.text().contains(SECRET));
      verifyNoInteractions(http, admission);
      assertThrows(
          RuntimeException.class,
          () ->
              client.sendHttpBytes(
                  endpoint,
                  SimpleHttpClient.HttpMethodType.POST,
                  new byte[] {1},
                  headers,
                  HttpClient.Version.HTTP_2));
      verifyNoInteractions(http);
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.CsvSource({
    "false,true",
    "true,true",
    "false,false",
    "true,false"
  })
  void boundedBytesStopDuringPreparationNeverAdmitsOrFabricatesARejection(
      boolean interrupted, boolean allowed) {
    HttpClient http = mock();
    var client = new SimpleHttpClient(3, 0, http, new LimitedSizeBodyHandler(1024));
    var stopped = new java.util.concurrent.atomic.AtomicBoolean();
    Runnable admission = mock();
    try (var security = mockStatic(SecurityUtils.class)) {
      security
          .when(() -> SecurityUtils.isUrlAllowed(URL))
          .thenAnswer(
              invocation -> {
                if (interrupted) Thread.currentThread().interrupt();
                else stopped.set(true);
                return allowed;
              });
      var hooks =
          hooks(
              () -> {
                if (stopped.get()) throw new ExecutionStoppedException("CANCELLED");
              });
      var thrown =
          assertThrows(
              Exception.class,
              () ->
                  client.sendHttpBytes(
                      URL,
                      SimpleHttpClient.HttpMethodType.POST,
                      new byte[] {1},
                      HEADERS,
                      HttpClient.Version.HTTP_2,
                      hooks,
                      admission));
      if (interrupted) assertInstanceOf(InterruptedException.class, thrown);
      else assertInstanceOf(ExecutionStoppedException.class, thrown);
      assertEquals(interrupted, Thread.currentThread().isInterrupted());
      verifyNoInteractions(http, admission);
    } finally {
      Thread.interrupted();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"io", "runtime", "interrupted"})
  void boundedBytesSendFailureOccursAfterAdmissionAndIsNotReclassifiedOrRetried(String kind)
      throws Exception {
    HttpClient http = mock();
    var handler = new LimitedSizeBodyHandler(1024);
    Exception failure =
        switch (kind) {
          case "io" -> new IOException(SECRET);
          case "runtime" -> new IllegalArgumentException(SECRET);
          default -> new InterruptedException(SECRET);
        };
    var order = new ArrayList<String>();
    when(http.send(any(), eq(handler)))
        .thenAnswer(
            invocation -> {
              order.add("send");
              throw failure;
            });
    var client = new SimpleHttpClient(3, 0, http, handler);
    try (var security = mockStatic(SecurityUtils.class);
        var logs = new Logs()) {
      security
          .when(() -> SecurityUtils.isUrlAllowed(URL))
          .thenAnswer(
              invocation -> {
                order.add("prepare");
                return true;
              });
      var thrown =
          assertThrows(
              Exception.class,
              () ->
                  client.sendHttpBytes(
                      URL,
                      SimpleHttpClient.HttpMethodType.POST,
                      new byte[] {1},
                      HEADERS,
                      HttpClient.Version.HTTP_2,
                      hooks(() -> order.add("checkpoint")),
                      () -> order.add("admit")));
      assertSame(failure, thrown);
      assertEquals(List.of("prepare", "checkpoint", "admit", "send"), order);
      verify(http).send(any(), eq(handler));
      assertFalse(logs.text().contains(SECRET));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"connection closed", "RST_STREAM"})
  void boundedTransportFailureHasNoRetryFallbackOrRawLogAndOrdinaryStillRetries(String reason)
      throws Exception {
    HttpClient http = mock();
    var handler = new LimitedSizeBodyHandler(1024);
    when(http.send(any(), eq(handler)))
        .thenThrow(new IOException(reason + " " + SECRET, new IllegalStateException(ROW)));
    var client = new SimpleHttpClient(3, 0, http, handler);
    try (var logs = new Logs()) {
      var failure =
          assertThrows(
              RuntimeException.class,
              () ->
                  client.callHttpEndpoint(
                      URL, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS, hooks(() -> {})));
      assertEquals("HTTP request failed", failure.getMessage());
      verify(http, times(1)).send(any(), eq(handler));
      assertFalse(logs.text().contains(SECRET));
      assertFalse(logs.text().contains(ROW));
      clearInvocations(http);
      assertThrows(
          RuntimeException.class,
          () -> client.callHttpEndpoint(URL, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS));
      verify(http, times(reason.equals("RST_STREAM") ? 2 : 3)).send(any(), eq(handler));
      assertTrue(logs.text().contains(SECRET));
      assertTrue(logs.text().contains(ROW));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 500})
  void boundedNegativeResponseDoesNotLogHeadersBodyOrUrl(int status) throws Exception {
    HttpClient http = mock();
    HttpResponse<String> response = mock();
    var handler = new LimitedSizeBodyHandler(1024);
    when(response.statusCode()).thenReturn(status);
    when(response.body()).thenReturn(SECRET);
    when(http.send(any(), eq(handler))).thenReturn(response);
    var client = new SimpleHttpClient(3, 0, http, handler);
    try (var logs = new Logs()) {
      var failure =
          assertThrows(
              RuntimeException.class,
              () ->
                  client.callHttpEndpoint(
                      URL, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS, hooks(() -> {})));
      assertEquals("HTTP request failed with status " + status, failure.getMessage());
      var request = org.mockito.ArgumentCaptor.forClass(HttpRequest.class);
      verify(http, times(1)).send(request.capture(), eq(handler));
      assertEquals(HttpClient.Version.HTTP_2, request.getValue().version().orElseThrow());
      assertFalse(logs.text().contains(SECRET));
      assertFalse(logs.text().contains(ROW));
      assertThrows(
          RuntimeException.class,
          () -> client.callHttpEndpoint(URL, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS));
      verify(http, times(2)).send(any(), eq(handler));
      assertTrue(logs.text().contains(SECRET));
      assertTrue(logs.text().contains(ROW));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stoppedControlOrInterruptedThreadCannotSend(boolean interrupted) {
    HttpClient http = mock();
    var client = new SimpleHttpClient(3, 0, http, new LimitedSizeBodyHandler(1024));
    try {
      if (interrupted) Thread.currentThread().interrupt();
      var control =
          hooks(
              () -> {
                if (!interrupted) throw new ExecutionStoppedException("CANCELLED");
              });
      assertThrows(
          ExecutionStoppedException.class,
          () ->
              client.callHttpEndpoint(
                  URL, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS, control));
      assertEquals(interrupted, Thread.currentThread().isInterrupted());
      verifyNoInteractions(http);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void interruptedHttpPreservesInterruptAndDoesNotRetry() throws Exception {
    HttpClient http = mock();
    var handler = new LimitedSizeBodyHandler(1024);
    when(http.send(any(), eq(handler))).thenThrow(new InterruptedException(SECRET));
    var client = new SimpleHttpClient(3, 0, http, handler);
    try {
      assertThrows(
          ExecutionStoppedException.class,
          () ->
              client.callHttpEndpoint(
                  URL, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS, hooks(() -> {})));
      assertTrue(Thread.currentThread().isInterrupted());
      verify(http, times(1)).send(any(), eq(handler));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void boundedEndpointRetainsUrlSecurityCheck() {
    HttpClient http = mock();
    var client = new SimpleHttpClient(3, 0, http, new LimitedSizeBodyHandler(1024));
    assertThrows(
        SecurityException.class,
        () ->
            client.callHttpEndpoint(
                "http://127.0.0.1/private",
                SimpleHttpClient.HttpMethodType.POST,
                ROW,
                HEADERS,
                hooks(() -> {})));
    verifyNoInteractions(http);
  }

  @Test
  void acceptedRequestWithLostResponseIsSentOnceOverRealHttp() throws Exception {
    List<byte[]> received = new CopyOnWriteArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/ingest",
        exchange -> {
          received.add(exchange.getRequestBody().readAllBytes());
          exchange.close();
        });
    server.start();
    String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/ingest";
    try (var security = mockStatic(SecurityUtils.class);
        var http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
      security.when(() -> SecurityUtils.isUrlAllowed(endpoint)).thenReturn(true);
      var client = new SimpleHttpClient(3, 0, http, new LimitedSizeBodyHandler(1024));
      assertThrows(
          RuntimeException.class,
          () ->
              client.callHttpEndpoint(
                  endpoint, SimpleHttpClient.HttpMethodType.POST, ROW, HEADERS, hooks(() -> {})));
      assertEquals(1, received.size());
      assertEquals(ROW, new String(received.getFirst(), java.nio.charset.StandardCharsets.UTF_8));
    } finally {
      server.stop(0);
    }
  }

  private static ExecutionHooks hooks(ExecutionControl control) {
    return new ExecutionHooks(control, mock(ExecutionHooks.Effects.class), Runnable::run);
  }

  private static final class Logs implements AutoCloseable {
    private final Logger logger = (Logger) LogManager.getLogger(SimpleHttpClient.class);
    private final Level previous = logger.getLevel();
    private final List<LogEvent> events = new ArrayList<>();
    private final AbstractAppender appender =
        new AbstractAppender("bounded-http", null, null, false, null) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };

    private Logs() {
      appender.start();
      logger.addAppender(appender);
      logger.setLevel(Level.ALL);
    }

    private String text() {
      var text = new StringWriter();
      var writer = new PrintWriter(text);
      for (var event : events) {
        writer.println(event.getMessage().getFormattedMessage());
        if (event.getThrown() != null) event.getThrown().printStackTrace(writer);
      }
      return text.toString();
    }

    @Override
    public void close() {
      logger.removeAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }
}
