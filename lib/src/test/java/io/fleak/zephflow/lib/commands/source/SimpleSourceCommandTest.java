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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
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
    private final SourceExecutionContext<SerializedEvent> testExecutionContext;

    TestSimpleSourceCommand(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator,
        String commandName,
        SourceExecutionContext<SerializedEvent> testExecutionContext) {
      super(nodeId, jobContext, configParser, configValidator);
      this.commandName = commandName;
      this.testExecutionContext = testExecutionContext;
    }

    @Override
    public String commandName() {
      return commandName;
    }

    @Override
    public SourceType sourceType() {
      return SourceType.BATCH;
    }

    @Override
    protected SourceExecutionContext<SerializedEvent> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return testExecutionContext;
    }
  }

  private static final String TEST_NODE_ID = "testNode";
  private static final String TEST_COMMAND_NAME = "testCommand";
  private static final Map<String, Object> TEST_CONFIG = Map.of("k", "test-config");

  private JobContext mockJobContext;
  private ConfigParser mockConfigParser;
  private ConfigValidator mockConfigValidator;
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
  private CommitStrategy mockCommitStrategy;
  private SourceExecutionContext<SerializedEvent> testExecutionContext;

  @BeforeEach
  void setUp() {
    mockJobContext = mock(JobContext.class);
    mockConfigParser = mock(ConfigParser.class);
    mockConfigValidator = mock(ConfigValidator.class);
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
    mockCommitStrategy = mock(CommitStrategy.class);

    when(mockFetcher.committer()).thenReturn(mockCommitter);
    when(mockFetcher.commitStrategy()).thenReturn(mockCommitStrategy);

    testExecutionContext =
        new SourceExecutionContext<>(
            mockFetcher,
            rawDataConverter,
            mockEncoder,
            dataSizeCounter,
            inputEventCounter,
            deserializeFailureCounter,
            mockDlqWriter);

    when(mockConfigParser.parseConfig(TEST_CONFIG)).thenReturn(mockCommandConfig);
    command =
        new TestSimpleSourceCommand(
            TEST_NODE_ID,
            mockJobContext,
            mockConfigParser,
            mockConfigValidator,
            TEST_COMMAND_NAME,
            testExecutionContext);
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

    // Setup commit strategy
    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    // Execute
    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

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

    // Setup commit strategy
    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    // Execute
    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

    verify(mockEncoder).serialize(same(serializedEvent));
    // Verify DLQ writing
    verify(mockDlqWriter)
        .writeToDlq(
            anyLong(), same(serializedEvent), contains("Test processing error"), eq("unknown"));
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

    // Setup commit strategy
    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

    verify(mockEncoder).serialize(same(serializedEvent));
    verify(mockDlqWriter)
        .writeToDlq(
            anyLong(),
            same(serializedEvent),
            contains("Test deserialization error"),
            eq(TEST_NODE_ID));
    verify(dataSizeCounter).increase(9, Map.of());
    verify(inputEventCounter, never()).increase(anyLong(), anyMap());
    verify(inputEventCounter, never()).increase(anyMap());
    verify(deserializeFailureCounter).increase(Map.of());
  }

  @Test
  void testExecuteWithDownstreamNodeError() throws Exception {
    SerializedEvent serializedEvent = new SerializedEvent(null, "test data".getBytes(), null);
    RecordFleakData mockRecord = mock(RecordFleakData.class);

    List<SerializedEvent> fetched = List.of(serializedEvent);
    when(mockDeserializer.deserialize(same(serializedEvent))).thenReturn(List.of(mockRecord));

    NodeExecutionException downstreamError =
        new NodeExecutionException(
            "parseNode", "parser", "Parse failed", new RuntimeException("Parse failed"));
    doThrow(downstreamError).when(mockSourceEventAcceptor).accept(any());

    when(mockFetcher.fetch())
        .thenAnswer(
            invocation -> {
              command.terminate();
              return fetched;
            });

    when(mockEncoder.serialize(same(serializedEvent))).thenReturn(serializedEvent);

    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

    verify(mockDlqWriter)
        .writeToDlq(anyLong(), same(serializedEvent), contains("Parse failed"), eq("parseNode"));
  }

  @Test
  void testCommandName() {
    assertEquals(TEST_COMMAND_NAME, command.commandName());
  }

  @Test
  void testFetcherExhaustedStopsLoop() throws Exception {
    // Setup fetcher to signal exhausted after first fetch
    when(mockFetcher.fetch()).thenReturn(List.of());
    when(mockFetcher.isExhausted()).thenReturn(true);

    // Setup commit strategy
    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    // Execute
    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

    // Verify only one fetch due to isExhausted()
    verify(mockFetcher, times(1)).fetch();
    verify(mockSourceEventAcceptor).terminate();
    verify(dataSizeCounter, never()).increase(anyLong(), anyMap());
    verify(dataSizeCounter, never()).increase(anyMap());
    verify(inputEventCounter, never()).increase(anyLong(), anyMap());
    verify(inputEventCounter, never()).increase(anyMap());
    verify(deserializeFailureCounter, never()).increase(anyLong(), anyMap());
    verify(deserializeFailureCounter, never()).increase(anyMap());
  }

  @Test
  void testFetcherExhaustedWithDataProcessesBeforeExit() throws Exception {
    // Setup test data
    RecordFleakData mockRecord = mock(RecordFleakData.class);
    SerializedEvent mockRaw = new SerializedEvent(null, "abcdef".getBytes(), null);
    List<SerializedEvent> fetched = List.of(mockRaw);

    when(mockFetcher.fetch()).thenReturn(fetched);
    when(mockFetcher.isExhausted()).thenReturn(true);
    when(mockDeserializer.deserialize(same(mockRaw))).thenReturn(List.of(mockRecord));

    // Setup commit strategy
    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    // Execute
    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

    // Verify data was processed before exit
    verify(mockFetcher, times(1)).fetch();
    verify(mockSourceEventAcceptor).accept(Collections.singletonList(mockRecord));
    verify(mockCommitter).commit();
    verify(mockSourceEventAcceptor).terminate();
  }

  @Test
  void testNoDlqConfigReturnsNoOpSampler() throws Exception {
    // No DLQ config set — sampler should be no-op and not cause issues
    when(mockJobContext.getDlqConfig()).thenReturn(null);

    RecordFleakData mockRecord = mock(RecordFleakData.class);
    SerializedEvent mockRaw = new SerializedEvent(null, "abcdef".getBytes(), null);
    when(mockFetcher.fetch())
        .thenAnswer(
            i -> {
              command.terminate();
              return List.of(mockRaw);
            });
    when(mockDeserializer.deserialize(same(mockRaw))).thenReturn(List.of(mockRecord));
    when(mockCommitStrategy.getCommitMode()).thenReturn(CommitStrategy.CommitMode.BATCH);
    when(mockCommitStrategy.shouldCommitNow(anyInt(), anyLong())).thenReturn(false);

    command.initialize(mockMetricClientProvider);
    command.execute("testUser", mockSourceEventAcceptor);

    verify(mockSourceEventAcceptor).accept(Collections.singletonList(mockRecord));
    verify(mockSourceEventAcceptor).terminate();
  }
}
