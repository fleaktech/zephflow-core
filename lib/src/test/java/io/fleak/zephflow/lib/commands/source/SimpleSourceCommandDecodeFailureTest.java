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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleSourceCommandDecodeFailureTest {

  private static final String NODE_ID = "source";
  private static final String TEXT = "MALFORMED json message: X\nAction: do this\ntrace";

  private final SerializedEvent bad = event("bad");
  private final SerializedEvent good = event("good");
  private final RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("a", 1));

  private Fetcher<SerializedEvent> fetcher;
  private Fetcher.Committer committer;
  private RawDataEncoder<SerializedEvent> encoder;
  private DlqWriter dlqWriter;
  private SourceEventAcceptor acceptor;

  @BeforeEach
  void setUp() throws Exception {
    fetcher = mock();
    committer = mock();
    encoder = mock();
    dlqWriter = mock();
    acceptor = mock();
    when(fetcher.committer()).thenReturn(committer);
    when(fetcher.commitStrategy()).thenReturn(BatchCommitStrategy.ofBatchSize(100));
    when(encoder.serialize(any())).thenAnswer(i -> i.getArgument(0));
  }

  @Test
  void decodeFailureIsDeadLetteredWithItsText() throws Exception {
    run(decodeFailure(), dlqWriter, List.of(bad, good));

    verify(dlqWriter).writeToDlq(anyLong(), same(bad), eq(TEXT), eq(NODE_ID));
    verify(acceptor).accept(List.of(record));
    verify(committer).commit();
  }

  @Test
  void failedWriteOfDecodeFailureIsLoggedAndTheSourceGoesOn() throws Exception {
    doThrow(new IllegalStateException("dlq down"))
        .when(dlqWriter)
        .writeToDlq(anyLong(), same(bad), anyString(), anyString());

    run(decodeFailure(), dlqWriter, List.of(bad, good));

    verify(acceptor).accept(List.of(record));
    verify(committer).commit();
    verify(acceptor).terminate();
  }

  @Test
  void decodeFailureWithoutDlqIsDropped() throws Exception {
    run(decodeFailure(), null, List.of(bad, good));

    verify(acceptor).accept(List.of(record));
    verify(committer).commit();
  }

  @Test
  void otherConversionFailureKeepsTheStackTrace() throws Exception {
    run(otherFailure(), dlqWriter, List.of(bad, good));

    verify(dlqWriter)
        .writeToDlq(
            anyLong(),
            same(bad),
            startsWith(
                "io.fleak.zephflow.lib.commands.NodeExecutionException: legacy failure\n"
                    + "\tat io.fleak.zephflow.lib.commands.source.SimpleSourceCommand"),
            eq(NODE_ID));
    verify(acceptor).accept(List.of(record));
  }

  @Test
  void failedWriteOfOtherFailureStillEndsTheSource() throws Exception {
    doThrow(new IllegalStateException("dlq down"))
        .when(dlqWriter)
        .writeToDlq(anyLong(), same(bad), anyString(), anyString());

    run(otherFailure(), dlqWriter, List.of(bad, good));

    verify(acceptor, never()).accept(any());
    verify(committer, never()).commit();
    verify(acceptor).terminate();
  }

  private RawDataConverter<SerializedEvent> decodeFailure() {
    return (event, context) ->
        event == bad
            ? ConvertedResult.failure(new MessageDecodeException(TEXT, null), event)
            : ConvertedResult.success(List.of(record), event);
  }

  private RawDataConverter<SerializedEvent> otherFailure() {
    return (event, context) ->
        event == bad
            ? ConvertedResult.failure(new IllegalArgumentException("legacy failure"), event)
            : ConvertedResult.success(List.of(record), event);
  }

  private void run(
      RawDataConverter<SerializedEvent> converter, DlqWriter dlq, List<SerializedEvent> fetched) {
    TestSource command =
        new TestSource(
            new SourceExecutionContext<>(fetcher, converter, encoder, null, null, null, dlq));
    when(fetcher.fetch())
        .thenAnswer(
            i -> {
              command.terminate();
              return fetched;
            });
    command.parseAndValidateArg(Map.of());
    command.initialize(mock(MetricClientProvider.class));
    command.execute("user", acceptor);
  }

  private static SerializedEvent event(String value) {
    return new SerializedEvent(null, value.getBytes(StandardCharsets.UTF_8), null);
  }

  private static class TestSource extends SimpleSourceCommand<SerializedEvent> {
    private final SourceExecutionContext<SerializedEvent> context;

    TestSource(SourceExecutionContext<SerializedEvent> context) {
      super(NODE_ID, mock(JobContext.class), config -> mock(CommandConfig.class), (c, n, j) -> {});
      this.context = context;
    }

    @Override
    public String commandName() {
      return "test";
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    protected SourceExecutionContext<SerializedEvent> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return context;
    }
  }
}
