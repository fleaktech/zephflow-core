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
package io.fleak.zephflow.lib.commands.fssource;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_SIZE_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_FILE_SKIPPED_COUNT;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackend;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * FsSource over a remote backend (a fake one under the {@code gs} scheme): each file is copied into
 * the spool and the copy deleted before the next, or streamed when over localSpoolMaxBytes, and a
 * read failure holds the file for the next run.
 */
class FsSourceRemoteBackendTest {

  private static final String CHECK_BACKEND =
      "Check the backend's read permission, the network, and free disk where the task runs";

  /** The files the fake backend lists, by name, and what each open of them returns. */
  private final Map<String, Supplier<InputStream>> files = new LinkedHashMap<>();

  private final Map<String, Integer> sizes = new LinkedHashMap<>();
  private final List<String> opened = new ArrayList<>();
  private final List<Long> spoolFilesAtOpen = new ArrayList<>();
  private final List<Integer> batches = new ArrayList<>();
  private final Map<String, Long> counters = new TreeMap<>();
  private FsSourceCommand command;

  @BeforeEach
  void registerFakeBackend() {
    FsBackendRegistry.unregister("gs");
    FsBackendRegistry.register(new FakeBackend());
  }

  @AfterEach
  void restoreBackend() {
    FsBackendRegistry.unregister("gs");
    FsBackendRegistry.register(new GcsBackend());
  }

  @Test
  void eachFileIsCopiedAndItsCopyDeletedBeforeTheNextOne() throws Exception {
    Set<Path> spoolsBefore = spools();
    file("f_1.jsonl", lines(1, 3));
    file("f_2.jsonl", lines(4, 6));

    run(Long.MAX_VALUE);

    assertEquals(List.of(3, 3), batches);
    assertEquals(List.of("gs://b/f_1.jsonl", "gs://b/f_2.jsonl"), opened);
    assertEquals(List.of(0L, 0L), spoolFilesAtOpen);
    assertEquals(spoolsBefore, spools(), "the spool is deleted when the run ends");
  }

  @Test
  void aStreamThatBreaksHoldsTheFileDropsTheOpenBatchAndIsReadAgain() throws Exception {
    String content = lines(1, 1500);
    files.put(
        "f_1.jsonl",
        () ->
            new SequenceInputStream(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                new InputStream() {
                  @Override
                  public int read() throws IOException {
                    throw new IOException("connection reset");
                  }
                }));
    sizes.put("f_1.jsonl", content.length() + 100);

    for (int run = 0; run < 2; run++) {
      batches.clear();
      counters.clear();
      try (BoundedLogCapture logs = new BoundedLogCapture(FsFileDecoder.class)) {
        assertThrows(IllegalStateException.class, () -> run(16));

        assertEquals(
            List.of(
                "fs_source skip file urn=gs://b/f_1.jsonl due to a read failure; it is read again"
                    + " on every run until it is fixed. "
                    + CHECK_BACKEND),
            errors(logs));
      }
      assertEquals(List.of(1000), batches);
      assertEquals(
          Map.of(
              METRIC_NAME_INPUT_EVENT_COUNT,
              1000L,
              METRIC_NAME_INPUT_EVENT_SIZE_COUNT,
              (long) lines(1, 1000).length() - 1,
              METRIC_NAME_INPUT_FILE_SKIPPED_COUNT + "{skip_reason=read_error}",
              1L),
          counters);
    }
  }

  @Test
  void aDownloadThatKeepsFailingIsAReadFailureAndTheNextFileIsStillRead() throws Exception {
    files.put(
        "f_1.jsonl",
        () ->
            new InputStream() {
              @Override
              public int read() throws IOException {
                throw new IOException("connection reset");
              }
            });
    sizes.put("f_1.jsonl", 10);
    file("f_2.jsonl", lines(1, 2));

    try (BoundedLogCapture logs = new BoundedLogCapture(FsFileDecoder.class)) {
      run(Long.MAX_VALUE);

      assertEquals(
          List.of(
              "fs_source skip file urn=gs://b/f_1.jsonl due to a read failure; it is read again on"
                  + " every run until it is fixed. "
                  + CHECK_BACKEND),
          errors(logs));
    }
    assertEquals(List.of(2), batches);
    assertEquals(
        List.of("gs://b/f_1.jsonl", "gs://b/f_1.jsonl", "gs://b/f_1.jsonl", "gs://b/f_2.jsonl"),
        opened);
    assertEquals(
        Map.of(
            METRIC_NAME_INPUT_EVENT_COUNT,
            2L,
            METRIC_NAME_INPUT_EVENT_SIZE_COUNT,
            (long) lines(1, 2).length() - 1,
            METRIC_NAME_INPUT_FILE_SKIPPED_COUNT + "{skip_reason=read_error}",
            1L),
        counters);
  }

  private void run(long localSpoolMaxBytes) throws Exception {
    if (command == null) {
      command = new FsSourceCommand("source", JobContext.builder().build());
      command.parseAndValidateArg(
          Map.of(
              "backend", "gs",
              "root", "gs://b/",
              "fileNameRegex", "f_(?<ts>\\d+)\\.jsonl",
              "encodingType", "JSON_OBJECT_LINE"));
      command.initialize(new RecordingMetricClientProvider());
    }
    ((FsSourceExecutionContext) command.getExecutionContext()).localSpoolMaxBytes =
        localSpoolMaxBytes;
    command.execute(
        "user",
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> batch) {
            batches.add(batch.size());
          }

          @Override
          public void terminate() {}
        });
  }

  private void file(String name, String content) {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    files.put(name, () -> new ByteArrayInputStream(bytes));
    sizes.put(name, bytes.length);
  }

  private static String lines(int first, int last) {
    return IntStream.rangeClosed(first, last)
        .mapToObj(i -> "{\"i\":" + i + "}\n")
        .collect(Collectors.joining());
  }

  private static List<String> errors(BoundedLogCapture logs) {
    return logs.events().stream()
        .filter(e -> e.getLevel() == Level.ERROR)
        .map(LogEvent::getMessage)
        .map(Message::getFormattedMessage)
        .toList();
  }

  /** The spool directories under the working directory, where FsSource creates its spool. */
  private static Set<Path> spools() throws IOException {
    try (Stream<Path> dirs = Files.list(Path.of(System.getProperty("user.dir")))) {
      return dirs.filter(d -> d.getFileName().toString().startsWith("fs-spool-"))
          .collect(Collectors.toSet());
    }
  }

  private static long spoolFiles() {
    try {
      long count = 0;
      for (Path spool : spools()) {
        try (Stream<Path> copies = Files.list(spool)) {
          count += copies.count();
        }
      }
      return count;
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private final class FakeBackend implements FsBackend {
    @Override
    public String scheme() {
      return "gs";
    }

    @Override
    public FileLister createLister(FsBackendConfig cfg) {
      return new FileLister() {
        @Override
        public Stream<FileEntry> list(ListRequest req) {
          return files.keySet().stream().map(this::entry);
        }

        @Override
        public FileEntry stat(FileKey key) {
          return entry(key.urn().substring("gs://b/".length()));
        }

        private FileEntry entry(String name) {
          return new FileEntry(
              new FileKey("gs", "gs://b/" + name), sizes.get(name), Instant.EPOCH, name);
        }
      };
    }

    @Override
    public FileReader createReader(FsBackendConfig cfg) {
      return (key, offset) -> {
        opened.add(key.urn());
        spoolFilesAtOpen.add(spoolFiles());
        return files.get(key.urn().substring("gs://b/".length())).get();
      };
    }

    @Override
    public Set<Capability> capabilities() {
      return Set.of();
    }
  }

  /** Counts by metric name, and for skipped files by name and reason. */
  private class RecordingMetricClientProvider implements MetricClientProvider {
    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          increase(1, additionalTags);
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          if (n == 0) {
            return;
          }
          String key =
              name.equals(METRIC_NAME_INPUT_FILE_SKIPPED_COUNT)
                  ? name + new TreeMap<>(additionalTags)
                  : name;
          counters.merge(key, n, Long::sum);
        }
      };
    }

    @Override
    public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
      return new MetricClientProvider.NoopMetricClientProvider().gauge(name, tags, monitoredValue);
    }

    @Override
    public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
      return new MetricClientProvider.NoopMetricClientProvider().stopWatch(name, tags);
    }

    @Override
    public void close() {}
  }
}
