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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SimpleSourceCommandCommitTest {

  @Mock private MetricClientProvider metricClientProvider;
  @Mock private SourceEventAcceptor sourceEventAcceptor;
  @Mock private DlqWriter dlqWriter;
  @Mock private RawDataEncoder<SerializedEvent> encoder;
  @Mock private RawDataConverter<SerializedEvent> converter;

  private AtomicInteger commitCount;
  private Fetcher.Committer mockCommitter;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    commitCount = new AtomicInteger(0);
    mockCommitter = () -> commitCount.incrementAndGet();
  }

  @Test
  void testPerRecordCommitStrategy() throws Exception {
    System.out.println("Starting testPerRecordCommitStrategy");
    TestSourceCommand command = new TestSourceCommand();
    List<SerializedEvent> testData = createTestData(5);

    command.testProcessFetchedData(testData, PerRecordCommitStrategy.INSTANCE);

    // Should commit after each record (5 commits)
    System.out.println("Expected: 5 commits, Actual: " + commitCount.get());
    assertEquals(5, commitCount.get(), "Per-record strategy should commit after each record");
  }

  @Test
  void testBatchCommitStrategy() throws Exception {
    System.out.println("Starting testBatchCommitStrategy");
    TestSourceCommand command = new TestSourceCommand();
    List<SerializedEvent> testData = createTestData(7);

    command.testProcessFetchedData(testData, BatchCommitStrategy.ofBatchSize(3));

    // Should commit after 3 records, then after 3 more records, then final commit for remaining 1
    System.out.println("Expected: 3 commits, Actual: " + commitCount.get());
    assertEquals(
        3, commitCount.get(), "Batch strategy should commit every 3 records plus final commit");
  }

  @Test
  void testNoCommitStrategy() throws Exception {
    System.out.println("Starting testNoCommitStrategy");
    TestSourceCommand command = new TestSourceCommand();
    List<SerializedEvent> testData = createTestData(5);

    command.testProcessFetchedData(testData, NoCommitStrategy.INSTANCE);

    // Should never commit
    System.out.println("Expected: 0 commits, Actual: " + commitCount.get());
    assertEquals(0, commitCount.get(), "No-commit strategy should never commit");
  }

  private List<SerializedEvent> createTestData(int count) {
    SerializedEvent[] events = new SerializedEvent[count];
    for (int i = 0; i < count; i++) {
      events[i] = new SerializedEvent(null, ("test" + i).getBytes(), null);
    }
    return Arrays.asList(events);
  }

  private class TestSourceCommand extends SimpleSourceCommand<SerializedEvent> {

    public TestSourceCommand() {
      super("test", null, null, null, false);
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    public String commandName() {
      return "test-source";
    }

    @Override
    protected SourceExecutionContext<SerializedEvent> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return new SourceExecutionContext<>(null, converter, encoder, null, null, null, dlqWriter);
    }

    public void testProcessFetchedData(List<SerializedEvent> testData, CommitStrategy strategy)
        throws Exception {
      // Mock converter to return successful results
      when(converter.convert(any(), any()))
          .thenAnswer(
              invocation -> {
                SerializedEvent event = invocation.getArgument(0);
                return ConvertedResult.success(Arrays.asList(mock(RecordFleakData.class)), event);
              });

      List<ConvertedResult<SerializedEvent>> convertedResults =
          testData.stream().map(data -> converter.convert(data, null)).toList();

      // Use reflection to call the private processFetchedData method
      Method method =
          SimpleSourceCommand.class.getDeclaredMethod(
              "processFetchedData",
              List.class,
              SourceEventAcceptor.class,
              Fetcher.Committer.class,
              DlqWriter.class,
              RawDataEncoder.class,
              CommitStrategy.class);
      method.setAccessible(true);

      method.invoke(
          this, convertedResults, sourceEventAcceptor, mockCommitter, dlqWriter, encoder, strategy);
    }
  }
}
