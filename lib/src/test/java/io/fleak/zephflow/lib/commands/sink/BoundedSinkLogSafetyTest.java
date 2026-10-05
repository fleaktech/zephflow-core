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
package io.fleak.zephflow.lib.commands.sink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.core.util.BinaryData;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import io.fleak.zephflow.lib.commands.azureblobsink.*;
import io.fleak.zephflow.lib.commands.azureeventhubsink.AzureEventHubSinkFlusher;
import io.fleak.zephflow.lib.commands.azuremonitorsink.*;
import io.fleak.zephflow.lib.commands.gcssink.*;
import io.fleak.zephflow.lib.commands.jdbcsink.*;
import io.fleak.zephflow.lib.commands.pubsubsink.*;
import io.fleak.zephflow.lib.commands.smtpsink.*;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BoundedSinkLogSafetyTest {
  private static final String SECRET = "credential=private-provider-value";
  private static final String ROW = "private-execution-row";

  @ParameterizedTest
  @ValueSource(strings = {"gcs", "blob", "pubsub", "smtp", "monitor", "jdbc", "eventhub"})
  void boundedFailuresDoNotLogProviderCausesBodiesOrRecordsButNormalDiagnosticsRemain(String kind)
      throws Exception {
    var logger = (Logger) LogManager.getLogger(loggerClass(kind));
    Level previous = logger.getLevel();
    List<LogEvent> events = new ArrayList<>();
    var appender =
        new AbstractAppender("bounded-log-safety", null, null, false, null) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.ALL);
    try {
      invoke(kind, true);
      assertFalse(render(events).contains(SECRET));
      assertFalse(render(events).contains(ROW));
      events.clear();
      invoke(kind, false);
      assertTrue(
          render(events).contains(SECRET), "Normal diagnostics still report the provider failure");
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  private static Class<?> loggerClass(String kind) {
    return switch (kind) {
      case "gcs" -> GcsSinkFlusher.class;
      case "blob" -> AzureBlobSinkFlusher.class;
      case "pubsub" -> PubSubSinkFlusher.class;
      case "smtp" -> SmtpSinkFlusher.class;
      case "monitor" -> AzureMonitorSinkFlusher.class;
      case "jdbc" -> JdbcSinkFlusher.class;
      case "eventhub" -> AzureEventHubSinkFlusher.class;
      default -> throw new AssertionError(kind);
    };
  }

  private static void invoke(String kind, boolean bounded) throws Exception {
    RuntimeException failure =
        new IllegalStateException("provider failed", new IllegalArgumentException(SECRET));
    switch (kind) {
      case "gcs" -> {
        Storage storage = mock();
        when(storage.create(
                any(BlobInfo.class), any(byte[].class), any(Storage.BlobTargetOption[].class)))
            .thenThrow(failure);
        flush(
            new GcsSinkFlusher(storage, "bucket", "prefix"), new GcsOutboundMessage(ROW), bounded);
      }
      case "blob" -> {
        BlobContainerClient container = mock();
        BlobClient blob = mock();
        when(container.getBlobClient(anyString())).thenReturn(blob);
        doThrow(failure).when(blob).upload(any(BinaryData.class), eq(true));
        flush(
            new AzureBlobSinkFlusher(container, "prefix"),
            new AzureBlobOutboundMessage(ROW),
            bounded);
      }
      case "pubsub" -> {
        PublisherStub publisher = mock();
        UnaryCallable<PublishRequest, PublishResponse> callable = mock();
        when(publisher.publishCallable()).thenReturn(callable);
        when(callable.call(any(PublishRequest.class))).thenThrow(failure);
        flush(
            new PubSubSinkFlusher(publisher, "topic"),
            new PubSubOutboundMessage(ROW, null),
            bounded);
      }
      case "smtp" -> {
        try (var transport = mockStatic(Transport.class)) {
          transport
              .when(() -> Transport.send(any(Message.class), anyString(), anyString()))
              .thenThrow(
                  new MessagingException("provider failed", new IllegalArgumentException(SECRET)));
          flush(
              new SmtpSinkFlusher(Session.getInstance(new Properties()), "username", "password"),
              new PreparedEmail(
                  "from@example.com",
                  List.of("to@example.com"),
                  null,
                  "subject",
                  ROW,
                  "text/plain"),
              bounded);
        }
      }
      case "monitor" -> {
        EntraIdTokenProvider token = mock();
        when(token.getToken()).thenReturn("secret-token");
        HttpClient client = mock();
        HttpResponse<String> response = mock();
        when(response.statusCode()).thenReturn(400);
        when(response.body()).thenReturn(SECRET);
        when(client.send(
                any(), org.mockito.ArgumentMatchers.<HttpResponse.BodyHandler<String>>any()))
            .thenReturn(response);
        flush(
            new AzureMonitorSinkFlusher("https://example.com", "rule", "stream", token, client),
            new AzureMonitorSinkOutboundEvent("{\"message\":\"" + ROW + "\"}"),
            bounded);
      }
      case "jdbc" -> {
        var flusher =
            new JdbcSinkFlusher(
                "jdbc:postgresql:test",
                "user",
                "password",
                "events",
                "public",
                JdbcSinkDto.WriteMode.INSERT,
                List.of());
        Connection connection = mock();
        PreparedStatement statement = mock();
        when(connection.getAutoCommit()).thenReturn(true);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeBatch()).thenThrow(new SQLException("write failed"));
        doThrow(new SQLException("rollback failed", failure)).when(connection).rollback();
        doThrow(new SQLException("restore failed", failure)).when(connection).setAutoCommit(true);
        var field = JdbcSinkFlusher.class.getDeclaredField("connection");
        field.setAccessible(true);
        field.set(flusher, connection);
        assertThrows(
            SQLException.class,
            () -> flush(flusher, Map.<String, Object>of("message", ROW), bounded));
      }
      case "eventhub" -> {
        FleakSerializer<?> serializer = mock();
        when(serializer.serialize(anyList())).thenThrow(failure);
        var flusher =
            new AzureEventHubSinkFlusher(mock(EventHubProducerClient.class), serializer, null);
        flush(flusher, (RecordFleakData) FleakData.wrap(Map.of("message", ROW)), bounded);
      }
      default -> throw new AssertionError(kind);
    }
  }

  private static <T> void flush(SimpleSinkCommand.Flusher<T> flusher, T value, boolean bounded)
      throws Exception {
    var events = new SimpleSinkCommand.PreparedInputEvents<T>();
    events.add((RecordFleakData) FleakData.wrap(Map.of("message", ROW)), value);
    var result =
        bounded
            ? flusher.flushBounded(
                events, Map.of(), new ExecutionHooks(() -> {}, mock(), Runnable::run))
            : flusher.flush(events, Map.of());
    assertEquals(1, result.errorOutputList().size());
    assertEquals(ROW, result.errorOutputList().getFirst().inputEvent().unwrap().get("message"));
  }

  private static String render(List<LogEvent> events) {
    StringWriter text = new StringWriter();
    PrintWriter writer = new PrintWriter(text);
    for (var event : events) {
      writer.println(event.getMessage().getFormattedMessage());
      if (event.getThrown() != null) event.getThrown().printStackTrace(writer);
    }
    return text.toString();
  }
}
