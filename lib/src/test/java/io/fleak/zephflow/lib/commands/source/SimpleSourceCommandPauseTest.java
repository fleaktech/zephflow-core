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
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class SimpleSourceCommandPauseTest {

  private static final Map<String, Object> CONFIG = Map.of("k", "v");
  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private final List<String> calls = new CopyOnWriteArrayList<>();
  private final AtomicInteger fetched = new AtomicInteger();
  private final AtomicInteger accepted = new AtomicInteger();
  private final AtomicInteger committedThrough = new AtomicInteger();

  private final Fetcher<Integer> fetcher =
      new Fetcher<>() {
        @Override
        public List<Integer> fetch() {
          calls.add("fetch");
          return List.of(fetched.incrementAndGet());
        }

        @Override
        public Committer committer() {
          return new Committer() {
            @Override
            public void commit() {}

            @Override
            public void commitThrough(int handledRecords) {
              committedThrough.set(fetched.get());
            }
          };
        }

        @Override
        public CommitStrategy commitStrategy() {
          return PerRecordCommitStrategy.INSTANCE;
        }

        @Override
        public void pause() {
          calls.add("pause");
        }

        @Override
        public void resume() {
          calls.add("resume");
        }

        @Override
        public void close() {}
      };

  private final SourceEventAcceptor acceptor =
      new SourceEventAcceptor() {
        @Override
        public void accept(List<RecordFleakData> records) {
          accepted.incrementAndGet();
        }

        @Override
        public void beforeCommit() {
          calls.add("confirm");
        }

        @Override
        public void terminate() {}
      };

  @Test
  void pauseResumePauseThenStop() throws Exception {
    SimpleSourceCommand<Integer> source = source();
    Thread loop = new Thread(() -> source.execute("user", acceptor));
    loop.start();

    awaitTrue(() -> accepted.get() >= 2);
    source.requestPause();
    awaitTrue(source::isPaused);
    int fetchedAtPause = fetched.get();
    assertEquals(fetchedAtPause, accepted.get());
    assertEquals(fetchedAtPause, committedThrough.get());
    Thread.sleep(300);
    assertEquals(fetchedAtPause, fetched.get(), "a paused source does not fetch");
    assertEquals("pause", calls.getLast());
    assertEquals("confirm", calls.get(calls.size() - 2));

    source.requestResume();
    awaitTrue(() -> accepted.get() > fetchedAtPause + 1);
    assertFalse(source.isPaused());

    source.requestPause();
    awaitTrue(source::isPaused);
    assertEquals(fetched.get(), committedThrough.get());

    source.requestStop();
    loop.join(TIMEOUT.toMillis());
    assertFalse(loop.isAlive());
    assertEquals(
        List.of("pause", "resume", "pause"),
        calls.stream().filter(c -> c.equals("pause") || c.equals("resume")).toList());
  }

  @Test
  void resumeBeforeThePauseTakesEffectKeepsFetching() throws Exception {
    SimpleSourceCommand<Integer> source = source();
    source.requestPause();
    source.requestResume();
    Thread loop = new Thread(() -> source.execute("user", acceptor));
    loop.start();

    awaitTrue(() -> accepted.get() >= 3);
    source.requestStop();
    loop.join(TIMEOUT.toMillis());
    assertFalse(loop.isAlive());
    assertFalse(calls.contains("pause"));
  }

  @Test
  void stopWithoutPauseBehavesAsBefore() throws Exception {
    SimpleSourceCommand<Integer> source = source();
    Thread loop = new Thread(() -> source.execute("user", acceptor));
    loop.start();

    awaitTrue(() -> accepted.get() >= 2);
    assertTrue(source.requestStop());
    loop.join(TIMEOUT.toMillis());
    assertFalse(loop.isAlive());
    assertFalse(source.isPaused());
    assertFalse(calls.contains("pause"));
  }

  @Test
  void supportsPauseByDefault() {
    assertTrue(source().supportsPause());
  }

  private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      assertTrue(System.nanoTime() < deadline, "timed out");
      Thread.sleep(10);
    }
  }

  private SimpleSourceCommand<Integer> source() {
    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("k", "v"));
    RawDataConverter<Integer> converter =
        (sourceRecord, context) -> ConvertedResult.success(List.of(event), sourceRecord);
    SourceExecutionContext<Integer> context =
        new SourceExecutionContext<>(fetcher, converter, mock(), mock(), mock(), mock(), null);
    ConfigParser parser = mock();
    when(parser.parseConfig(CONFIG)).thenReturn(mock(CommandConfig.class));
    SimpleSourceCommand<Integer> source =
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
          protected SourceExecutionContext<Integer> createExecutionContext(
              MetricClientProvider metricClientProvider,
              JobContext jobContext,
              CommandConfig commandConfig,
              String nodeId) {
            return context;
          }
        };
    source.parseAndValidateArg(CONFIG);
    source.initialize(mock(MetricClientProvider.class));
    return source;
  }
}
