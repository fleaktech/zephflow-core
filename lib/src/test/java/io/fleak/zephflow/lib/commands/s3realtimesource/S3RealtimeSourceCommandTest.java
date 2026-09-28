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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceCommand.SourceType;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/** Exercises the full factory -> parse -> validate -> createExecutionContext wiring. */
class S3RealtimeSourceCommandTest {

  @Test
  void buildsStreamingExecutionContext() throws Exception {
    JobContext jobContext = TestUtils.buildJobContext(new HashMap<>());
    S3RealtimeSourceCommand command =
        new S3RealtimeSourceCommandFactory().createCommand("node", jobContext);

    Map<String, Object> config =
        Map.of(
            "queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789012/q",
            "regionStr", "us-east-1",
            "encodingType", "JSON_OBJECT_LINE");
    command.parseAndValidateArg(config);
    command.initialize(mock(MetricClientProvider.class));

    assertEquals(SourceType.STREAMING, command.sourceType());
    assertEquals("s3rtsource", command.commandName());
    assertInstanceOf(SourceExecutionContext.class, command.getExecutionContext());

    command.terminate();
  }

  @ParameterizedTest
  @ValueSource(strings = {"shared", "override", "default"})
  void realCommandUsesRecordRegionsAndPreservesCredentialsAndEndpoint(String mode)
      throws Exception {
    JobContext job = TestUtils.buildJobContext(new HashMap<>());
    Map<String, Object> config = config();
    UsernamePasswordCredential queueCredential = null;
    UsernamePasswordCredential objectCredential = null;
    if (!mode.equals("default")) {
      config.put("credentialId", "queue-credential");
      job.getOtherProperties()
          .put(
              "queue-credential",
              new HashMap<>(Map.of("username", "queue-key", "password", "queue-secret")));
      queueCredential = new UsernamePasswordCredential("queue-key", "queue-secret");
      objectCredential = queueCredential;
    }
    if (mode.equals("override")) {
      config.put("s3CredentialId", "s3-credential");
      job.getOtherProperties()
          .put(
              "s3-credential",
              new HashMap<>(Map.of("username", "s3-key", "password", "s3-secret")));
      objectCredential = new UsernamePasswordCredential("s3-key", "s3-secret");
    }
    config.put("s3RegionStr", "ap-south-1");
    config.put("s3EndpointOverride", "http://localhost:4566");
    AwsClientFactory factory = mock(AwsClientFactory.class);
    SqsClient sqs = mock(SqsClient.class);
    S3Client europe = mock(S3Client.class);
    S3Client west = mock(S3Client.class);
    when(factory.createSqsClient(eq("us-east-1"), eq(queueCredential))).thenReturn(sqs);
    when(factory.createS3Client(
            eq("eu-central-1"), eq(objectCredential), eq("http://localhost:4566")))
        .thenReturn(europe);
    when(factory.createS3Client(eq("us-west-2"), eq(objectCredential), eq("http://localhost:4566")))
        .thenReturn(west);
    when(europe.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> stream("{\"region\":\"europe\"}"));
    when(west.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> stream("{\"region\":\"west\"}"));

    S3RealtimeSourceCommand command = initialize(job, config, factory);
    verify(factory).createSqsClient("us-east-1", queueCredential);
    verify(factory, never()).createS3Client(any(), any(), any());
    SourceExecutionContext<S3EventMessage> context = context(command);
    S3EventMessage first =
        message(
            "first",
            List.of(
                new S3ObjectRef("b", "one", "eu-central-1"),
                new S3ObjectRef("b", "two", "us-west-2")));
    S3EventMessage second =
        message("second", List.of(new S3ObjectRef("b", "three", "eu-central-1")));
    assertEquals(2, context.converter().convert(first, context).transformedData().size());
    assertEquals(1, context.converter().convert(second, context).transformedData().size());
    verify(factory).createS3Client("eu-central-1", objectCredential, "http://localhost:4566");
    verify(factory).createS3Client("us-west-2", objectCredential, "http://localhost:4566");
    verifyNoMoreInteractions(factory);
    verify(europe, times(2)).getObject(any(GetObjectRequest.class));
    verify(west).getObject(any(GetObjectRequest.class));
    context.fetcher().committer().commit();
    verify(sqs)
        .deleteMessage(
            DeleteMessageRequest.builder()
                .queueUrl((String) config.get("queueUrl"))
                .receiptHandle("first")
                .build());
    verify(sqs)
        .deleteMessage(
            DeleteMessageRequest.builder()
                .queueUrl((String) config.get("queueUrl"))
                .receiptHandle("second")
                .build());
    command.terminate();
    verify(sqs).close();
    verify(europe).close();
    verify(west).close();
  }

  @Test
  void successfulContextCleanupClosesDlqAndDirectTerminatePropagatesCleanupFailure()
      throws Exception {
    JobContext job = TestUtils.buildJobContext(new HashMap<>());
    job.setDlqConfig(JobContext.S3DlqConfig.builder().build());
    DlqWriter dlq = mock(DlqWriter.class);
    AwsClientFactory factory = mock(AwsClientFactory.class);
    SqsClient sqs = mock(SqsClient.class);
    when(factory.createSqsClient(any(), any())).thenReturn(sqs);
    S3RealtimeSourceCommand command;
    try (var dlqFactory = mockStatic(DlqWriterFactory.class)) {
      dlqFactory
          .when(() -> DlqWriterFactory.createDlqWriter(job.getDlqConfig(), null))
          .thenReturn(dlq);
      command = initialize(job, config(), factory);
    }
    verify(dlq).open();
    RuntimeException cause = new IllegalStateException("sqs close");
    doThrow(cause).doNothing().when(sqs).close();
    RuntimeException failure = assertThrows(RuntimeException.class, command::terminate);
    assertSame(cause, failure.getCause());
    assertTrue(command.isInitialized());
    command.terminate();
    assertFalse(command.isInitialized());
    verify(sqs, times(2)).close();
    verify(dlq).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void executeLogsCompleteCleanupFailureAndFollowingTerminateRetries(boolean transientFailure)
      throws Exception {
    AwsClientFactory factory = mock(AwsClientFactory.class);
    SqsClient sqs = mock(SqsClient.class);
    S3Client europe = mock(S3Client.class);
    S3Client west = mock(S3Client.class);
    when(factory.createSqsClient(any(), any())).thenReturn(sqs);
    when(factory.createS3Client(eq("eu-central-1"), any(), any())).thenReturn(europe);
    when(factory.createS3Client(eq("us-west-2"), any(), any())).thenReturn(west);
    when(europe.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> stream("{\"ok\":1}"));
    when(west.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> stream("{\"ok\":2}"));
    RuntimeException firstCause = new IllegalStateException("first close failure");
    RuntimeException secondCause = new IllegalStateException("second close failure");
    if (transientFailure) {
      doThrow(firstCause).doNothing().when(europe).close();
      doThrow(secondCause).doNothing().when(west).close();
    } else {
      doThrow(firstCause).when(europe).close();
      doThrow(secondCause).when(west).close();
    }
    String notification =
        """
        {"Records":[
          {"eventName":"ObjectCreated:Put","awsRegion":"eu-central-1","s3":{"bucket":{"name":"b"},"object":{"key":"one"}}},
          {"eventName":"ObjectCreated:Put","awsRegion":"us-west-2","s3":{"bucket":{"name":"b"},"object":{"key":"two"}}}
        ]}
        """;
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(
            ReceiveMessageResponse.builder()
                .messages(
                    Message.builder().messageId("m").receiptHandle("r").body(notification).build())
                .build())
        .thenThrow(new IllegalStateException("stop test source"));
    S3RealtimeSourceCommand command =
        initialize(TestUtils.buildJobContext(new HashMap<>()), config(), factory);
    List<LogEvent> logs = new ArrayList<>();
    Logger logger = (Logger) LogManager.getLogger(SimpleSourceCommand.class);
    AbstractAppender appender =
        new AbstractAppender(
            "cleanup-test",
            null,
            PatternLayout.createDefaultLayout(),
            false,
            Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            logs.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      SourceEventAcceptor acceptor = mock(SourceEventAcceptor.class);
      command.execute("test", acceptor);
      verify(acceptor).accept(argThat(records -> records.size() == 2));
      List<LogEvent> cleanupLogs =
          logs.stream()
              .filter(
                  event ->
                      event
                          .getMessage()
                          .getFormattedMessage()
                          .equals("failed to terminate source command"))
              .toList();
      assertEquals(1, cleanupLogs.size());
      LogEvent event = cleanupLogs.getFirst();
      assertEquals(Level.ERROR, event.getLevel());
      assertNotNull(event.getThrown());
      assertTrue(event.getThrown().getMessage().contains("node"));
      Throwable regionalFailure = event.getThrown().getCause();
      assertTrue(regionalFailure.getMessage().contains("eu-central-1"));
      assertSame(firstCause, regionalFailure.getCause());
      assertEquals(1, regionalFailure.getSuppressed().length);
      assertTrue(regionalFailure.getSuppressed()[0].getMessage().contains("us-west-2"));
      assertSame(secondCause, regionalFailure.getSuppressed()[0].getCause());
      assertTrue(command.isInitialized());
      if (transientFailure) {
        assertDoesNotThrow(command::terminate);
        assertFalse(command.isInitialized());
      } else {
        RuntimeException repeatedFailure = assertThrows(RuntimeException.class, command::terminate);
        assertSame(firstCause, repeatedFailure.getCause().getCause());
        assertSame(secondCause, repeatedFailure.getCause().getSuppressed()[0].getCause());
        assertTrue(command.isInitialized());
      }
      verify(sqs, times(2)).close();
      verify(europe, times(2)).close();
      verify(west, times(2)).close();
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  @Test
  void retainsLegacyRegionDtoJsonAndPublicMethodsWithoutAcceptingUnknownFields() {
    JsonConfigParser<S3RealtimeSourceDto.Config> parser =
        new JsonConfigParser<>(S3RealtimeSourceDto.Config.class);
    Map<String, Object> input = config();
    input.put("s3RegionStr", "ap-south-1");
    S3RealtimeSourceDto.Config parsed = parser.parseConfig(input);
    assertEquals("ap-south-1", parsed.getS3RegionStr());
    S3RealtimeSourceDto.Config built =
        S3RealtimeSourceDto.Config.builder()
            .queueUrl("queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT_LINE)
            .s3RegionStr("us-west-2")
            .build();
    assertEquals("us-west-2", built.getS3RegionStr());
    built.setS3RegionStr("eu-central-1");
    assertEquals("eu-central-1", OBJECT_MAPPER.convertValue(built, Map.class).get("s3RegionStr"));
    input.put("unknownS3Region", "eu-west-1");
    assertThrows(IllegalArgumentException.class, () -> parser.parseConfig(input));
  }

  @ParameterizedTest
  @ValueSource(strings = {"download", "decode", "factory"})
  void closesCreatedClientsAfterObjectFailureAndKeepsGoodSibling(String failureMode)
      throws Exception {
    AwsClientFactory factory = mock(AwsClientFactory.class);
    SqsClient sqs = mock(SqsClient.class);
    S3Client europe = mock(S3Client.class);
    S3Client west = mock(S3Client.class);
    when(factory.createSqsClient(any(), any())).thenReturn(sqs);
    when(factory.createS3Client(eq("eu-central-1"), any(), any())).thenReturn(europe);
    when(europe.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> stream("{\"ok\":1}"));
    if (failureMode.equals("factory")) {
      when(factory.createS3Client(eq("us-west-2"), any(), any()))
          .thenThrow(new IllegalStateException("factory failed"));
    } else {
      when(factory.createS3Client(eq("us-west-2"), any(), any())).thenReturn(west);
      if (failureMode.equals("download")) {
        when(west.getObject(any(GetObjectRequest.class)))
            .thenThrow(new IllegalStateException("download failed"));
      } else {
        when(west.getObject(any(GetObjectRequest.class)))
            .thenAnswer(invocation -> stream("invalid JSON"));
      }
    }
    S3RealtimeSourceCommand command =
        initialize(TestUtils.buildJobContext(new HashMap<>()), config(), factory);
    SourceExecutionContext<S3EventMessage> context = context(command);
    S3EventMessage message =
        message(
            "receipt",
            List.of(
                new S3ObjectRef("b", "one", "eu-central-1"),
                new S3ObjectRef("b", "two", "us-west-2")));
    assertEquals(1, context.converter().convert(message, context).transformedData().size());
    command.terminate();
    verify(sqs).close();
    verify(europe).close();
    if (failureMode.equals("factory")) {
      verifyNoInteractions(west);
    } else {
      verify(west).close();
    }
  }

  private static Map<String, Object> config() {
    return new HashMap<>(
        Map.of(
            "queueUrl",
            "https://sqs.us-east-1.amazonaws.com/123456789012/q",
            "regionStr",
            "us-east-1",
            "encodingType",
            "JSON_OBJECT_LINE"));
  }

  private static S3RealtimeSourceCommand initialize(
      JobContext job, Map<String, Object> config, AwsClientFactory factory) throws Exception {
    S3RealtimeSourceCommand command =
        new S3RealtimeSourceCommand(
            "node",
            job,
            new JsonConfigParser<>(S3RealtimeSourceDto.Config.class),
            new S3RealtimeSourceConfigValidator(),
            factory);
    command.parseAndValidateArg(config);
    MetricClientProvider metrics = mock(MetricClientProvider.class);
    when(metrics.counter(anyString(), anyMap())).thenReturn(mock(FleakCounter.class));
    command.initialize(metrics);
    return command;
  }

  @SuppressWarnings("unchecked")
  private static SourceExecutionContext<S3EventMessage> context(S3RealtimeSourceCommand command) {
    return (SourceExecutionContext<S3EventMessage>) command.getExecutionContext();
  }

  private static S3EventMessage message(String receipt, List<S3ObjectRef> refs) {
    return new S3EventMessage("m-" + receipt, receipt, "raw-body", refs);
  }

  private static ResponseInputStream<GetObjectResponse> stream(String body) {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    return new ResponseInputStream<>(
        GetObjectResponse.builder().contentLength((long) bytes.length).build(),
        new ByteArrayInputStream(bytes));
  }
}
