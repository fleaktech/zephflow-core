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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleSourceCommandTest {

  // Test implementation of SimpleSourceCommand
  private static class TestSimpleSourceCommand extends SimpleSourceCommand<SerializedEvent> {
    private final String commandName;

    TestSimpleSourceCommand(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator,
        CommandInitializerFactory commandInitializerFactory,
        boolean singleEventSource,
        String commandName) {
      super(
          nodeId,
          jobContext,
          configParser,
          configValidator,
          commandInitializerFactory,
          singleEventSource);
      this.commandName = commandName;
    }

    @Override
    public String commandName() {
      return commandName;
    }

    @Override
    public SourceType sourceType() {
      return SourceType.BATCH;
    }
  }

  private static final String TEST_NODE_ID = "testNode";
  private static final String TEST_COMMAND_NAME = "testCommand";
  private static final String TEST_CONFIG = "test-config";

  private JobContext mockJobContext;
  private ConfigParser mockConfigParser;
  private ConfigValidator mockConfigValidator;
  private CommandInitializerFactory mockCommandInitializerFactory;
  private Fetcher<SerializedEvent> mockFetcher;
  private FleakDeserializer<?> mockDeserializer;
  private RawDataEncoder<SerializedEvent> mockEncoder;
  private Fetcher.Committer mockCommitter;
  private SourceEventAcceptor mockSourceEventAcceptor;
  private MetricClientProvider mockMetricClientProvider;
  private DlqWriter mockDlqWriter;
  private TestSimpleSourceCommand command;
  private CommandConfig mockCommandConfig;
  private FleakCounter dataSizeCounter;
  private FleakCounter inputEventCounter;
  private FleakCounter deserializeFailureCounter;

  @BeforeEach
  void setUp() {
    mockJobContext = mock(JobContext.class);
    mockConfigParser = mock(ConfigParser.class);
    mockConfigValidator = mock(ConfigValidator.class);
    mockCommandInitializerFactory = mock(CommandInitializerFactory.class);
    CommandInitializer mockCommandInitializer = mock(CommandInitializer.class);
    mockFetcher = mock();
    mockDeserializer = mock();
    RawDataConverter<SerializedEvent> rawDataConverter =
        new BytesRawDataConverter(mockDeserializer);
    mockEncoder = mock();
    mockSourceEventAcceptor = mock(SourceEventAcceptor.class);
    mockMetricClientProvider = mock(MetricClientProvider.class);
    mockDlqWriter = mock(DlqWriter.class);
    mockCommandConfig = mock(CommandConfig.class);
    mockCommitter = mock(Fetcher.Committer.class);
    dataSizeCounter = mock();
    inputEventCounter = mock();
    deserializeFailureCounter = mock();

    when(mockFetcher.commiter()).thenReturn(mockCommitter);
    // Setup command initializer factory
    when(mockCommandInitializerFactory.createCommandInitializer(
            any(), eq(mockJobContext), any(), eq(TEST_NODE_ID)))
        .thenReturn(mockCommandInitializer);

    // Setup command initializer to return config with our mocked fetcher
    when(mockCommandInitializer.initialize(
            eq(TEST_COMMAND_NAME), eq(mockJobContext), any(CommandConfig.class)))
        .thenReturn(
            new SourceInitializedConfig<>(
                mockFetcher,
                rawDataConverter,
                mockEncoder,
                dataSizeCounter,
                inputEventCounter,
                deserializeFailureCounter,
                mockDlqWriter));
    when(mockConfigParser.parseConfig(TEST_CONFIG)).thenReturn(mockCommandConfig);
    command =
        new TestSimpleSourceCommand(
            TEST_NODE_ID,
            mockJobContext,
            mockConfigParser,
            mockConfigValidator,
            mockCommandInitializerFactory,
            false,
            TEST_COMMAND_NAME);
    command.parseAndValidateArg(TEST_CONFIG);
  }

  @Test
  void testParseAndValidateArg() {
    verify(mockConfigValidator).validateConfig(mockCommandConfig, TEST_NODE_ID, mockJobContext);
  }

  @Test
  void testExecuteWithSuccessfulFetch() throws Exception {
    // Setup test data
    RecordFleakData mockRecord = mock(RecordFleakData.class);

    SerializedEvent mockRaw = new SerializedEvent(null, "abcdef".getBytes(), null);
    List<SerializedEvent> fetched = List.of(mockRaw);
    when(mockFetcher.fetch())
        .thenAnswer(
            i -> {
              command.terminate();
              return fetched;
            });
    when(mockDeserializer.deserialize(same(mockRaw))).thenReturn(List.of(mockRecord));

    // Execute
    command.execute("testUser", mockMetricClientProvider, mockSourceEventAcceptor);

    verify(mockCommandInitializerFactory)
        .createCommandInitializer(
            eq(mockMetricClientProvider), eq(mockJobContext), any(), eq(TEST_NODE_ID));

    verify(mockSourceEventAcceptor).accept(Collections.singletonList(mockRecord));
    verify(mockCommitter).commit();
    verify(mockSourceEventAcceptor).terminate();
    verify(dataSizeCounter).increase(6, Map.of());
    verify(inputEventCounter).increase(1, Map.of());
    verify(deserializeFailureCounter, never()).increase(anyLong(), anyMap());
    verify(deserializeFailureCounter, never()).increase(anyMap());
  }

  @Test
  void testExecuteWithProcessingError() throws Exception {
    SerializedEvent serializedEvent = new SerializedEvent(null, "test data".getBytes(), null);
    // Setup test data
    RecordFleakData mockRecord = mock(RecordFleakData.class);

    List<SerializedEvent> fetched = List.of(serializedEvent);
    when(mockDeserializer.deserialize(same(serializedEvent))).thenReturn(List.of(mockRecord));

    // Setup processing error
    RuntimeException testException = new RuntimeException("Test processing error");
    doThrow(testException).when(mockSourceEventAcceptor).accept(any());

    when(mockFetcher.fetch())
        .thenAnswer(
            invocation -> {
              command.terminate();
              return fetched;
            });

    when(mockEncoder.serialize(same(serializedEvent))).thenReturn(serializedEvent);

    // Execute
    command.execute("testUser", mockMetricClientProvider, mockSourceEventAcceptor);

    verify(mockEncoder).serialize(same(serializedEvent));
    // Verify DLQ writing
    verify(mockDlqWriter)
        .writeToDlq(anyLong(), same(serializedEvent), contains("Test processing error"));
    verify(dataSizeCounter).increase(9, Map.of());
    verify(inputEventCounter).increase(1, Map.of());
    verify(deserializeFailureCounter, never()).increase(anyLong(), anyMap());
    verify(deserializeFailureCounter, never()).increase(anyMap());
  }

  @Test
  public void testExecuteWithDeserError() throws Exception {
    SerializedEvent serializedEvent = new SerializedEvent(null, "test data".getBytes(), null);
    when(mockFetcher.fetch())
        .thenAnswer(
            invocation -> {
              command.terminate();
              return List.of(serializedEvent);
            });
    when(mockDeserializer.deserialize(any()))
        .thenThrow(new IllegalArgumentException("Test deserialization error"));
    when(mockEncoder.serialize(same(serializedEvent))).thenReturn(serializedEvent);
    command.execute("testUser", mockMetricClientProvider, mockSourceEventAcceptor);

    verify(mockEncoder).serialize(same(serializedEvent));
    verify(mockDlqWriter)
        .writeToDlq(anyLong(), same(serializedEvent), contains("Test deserialization error"));
    verify(dataSizeCounter).increase(9, Map.of());
    verify(inputEventCounter, never()).increase(anyLong(), anyMap());
    verify(deserializeFailureCounter).increase(Map.of());
  }

  @Test
  void testCommandName() {
    assertEquals(TEST_COMMAND_NAME, command.commandName());
  }

  @Test
  void testSingleEventSourceBehavior() throws Exception {
    // Create command with singleEventSource=true
    command =
        new TestSimpleSourceCommand(
            TEST_NODE_ID,
            mockJobContext,
            mockConfigParser,
            mockConfigValidator,
            mockCommandInitializerFactory,
            true,
            TEST_COMMAND_NAME);
    command.parseAndValidateArg(TEST_CONFIG);
    // Setup single successful fetch
    when(mockFetcher.fetch()).thenReturn(List.of());

    // Execute
    command.execute("testUser", mockMetricClientProvider, mockSourceEventAcceptor);

    // Verify only one fetch
    verify(mockFetcher, times(1)).fetch();
    verify(mockSourceEventAcceptor).terminate();
    verify(dataSizeCounter, never()).increase(anyLong(), anyMap());
    verify(dataSizeCounter, never()).increase(anyMap());
    verify(inputEventCounter, never()).increase(anyLong(), anyMap());
    verify(inputEventCounter, never()).increase(anyMap());
    verify(deserializeFailureCounter, never()).increase(anyLong(), anyMap());
    verify(deserializeFailureCounter, never()).increase(anyMap());
  }
}
