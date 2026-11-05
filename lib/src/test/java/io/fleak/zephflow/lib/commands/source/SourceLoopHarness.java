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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;

/** Runs the real {@link SimpleSourceCommand} loop over the first fetches of a fetcher. */
public final class SourceLoopHarness {

  private static final Map<String, Object> CONFIG = Map.of("k", "v");

  /** Thrown by an acceptor to stand for the process being killed while handling a record. */
  public static final class ProcessKilled extends Error {}

  private SourceLoopHarness() {}

  /** Each fetched record becomes one event. */
  public static <T> RawDataConverter<T> oneEventPerRecord() {
    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("k", "v"));
    return (sourceRecord, context) -> ConvertedResult.success(List.of(event), sourceRecord);
  }

  /**
   * Accepts records, counting from 1, and throws on those {@code fails} matches, so that they go to
   * the DLQ.
   */
  public static SourceEventAcceptor failing(IntPredicate fails) {
    return acceptor(fails, record -> false);
  }

  /**
   * Accepts records, counting from 1, and stands for a killed process at record {@code killedAt}.
   */
  public static SourceEventAcceptor killedAt(int killedAt) {
    return acceptor(record -> false, record -> record == killedAt);
  }

  private static SourceEventAcceptor acceptor(IntPredicate fails, IntPredicate kills) {
    return new SourceEventAcceptor() {
      private int accepted;

      @Override
      public void accept(List<RecordFleakData> records) {
        accepted++;
        if (kills.test(accepted)) {
          throw new ProcessKilled();
        }
        if (fails.test(accepted)) {
          throw new IllegalStateException("record " + accepted + " failed");
        }
      }

      @Override
      public void terminate() {}
    };
  }

  /** Handles the first fetch, with failed records going to {@code dlqWriter}. */
  public static <T> void run(
      Fetcher<T> fetcher,
      RawDataConverter<T> converter,
      SourceEventAcceptor acceptor,
      DlqWriter dlqWriter) {
    run(fetcher, 1, converter, acceptor, dlqWriter);
  }

  /**
   * Handles the first {@code fetches} fetches, with failed records going to {@code dlqWriter}. The
   * acceptor counts records across fetches.
   */
  public static <T> void run(
      Fetcher<T> fetcher,
      int fetches,
      RawDataConverter<T> converter,
      SourceEventAcceptor acceptor,
      DlqWriter dlqWriter) {
    SourceExecutionContext<T> context =
        new SourceExecutionContext<>(
            new LimitedFetches<>(fetcher, fetches),
            converter,
            mock(),
            mock(),
            mock(),
            mock(),
            dlqWriter);
    ConfigParser parser = mock();
    when(parser.parseConfig(CONFIG)).thenReturn(mock(CommandConfig.class));
    SimpleSourceCommand<T> source =
        new SimpleSourceCommand<>("source", mock(JobContext.class), parser, mock()) {
          @Override
          public SourceType sourceType() {
            return SourceType.STREAMING;
          }

          @Override
          public String commandName() {
            return "source";
          }

          @Override
          protected SourceExecutionContext<T> createExecutionContext(
              MetricClientProvider metricClientProvider,
              JobContext jobContext,
              CommandConfig commandConfig,
              String nodeId) {
            return context;
          }
        };
    source.parseAndValidateArg(CONFIG);
    source.initialize(mock(MetricClientProvider.class));
    source.execute("user", acceptor);
  }

  private static final class LimitedFetches<T> implements Fetcher<T> {
    private final Fetcher<T> fetcher;
    private final int fetches;
    private int fetched;

    LimitedFetches(Fetcher<T> fetcher, int fetches) {
      this.fetcher = fetcher;
      this.fetches = fetches;
    }

    @Override
    public List<T> fetch() {
      fetched++;
      return fetcher.fetch();
    }

    @Override
    public boolean isExhausted() {
      return fetched >= fetches;
    }

    @Override
    public CommitStrategy commitStrategy() {
      return fetcher.commitStrategy();
    }

    @Override
    public Committer committer() {
      return fetcher.committer();
    }

    @Override
    public void close() throws IOException {
      fetcher.close();
    }
  }
}
