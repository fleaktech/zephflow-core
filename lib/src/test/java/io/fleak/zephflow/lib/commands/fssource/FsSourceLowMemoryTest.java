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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DeadLetterCommiterSerializer;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * FsSource reading files far larger than a 64 MiB heap through the {@code file} backend, with a
 * real buffered DLQ writer (batch size 10): the S3 realtime low-memory test's shapes.
 */
@Tag("lowMemory")
class FsSourceLowMemoryTest {

  private static final long MIB = 1024L * 1024;
  private static final long TARGET_BYTES = 200 * MIB;
  private static final int DLQ_BATCH = 10;

  @TempDir Path root;

  private final AtomicLong records = new AtomicLong();
  private final AtomicLong batches = new AtomicLong();
  private final AtomicLong dlqEntries = new AtomicLong();
  private final AtomicLong dlqBytes = new AtomicLong();

  @BeforeAll
  static void heapIsSmallerThanTheInputs() {
    assertTrue(
        Runtime.getRuntime().maxMemory() < TARGET_BYTES / 2,
        "run through the lowMemoryTest task so the heap is bounded");
  }

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  @Test
  void jsonLinesWithManyShortFields() throws Exception {
    String line = jsonWithTwentyFields();
    long count = TARGET_BYTES / line.length();
    run("JSON_OBJECT_LINE", file("a.jsonl", count, i -> line, false));
    assertEquals(List.of(count, 0L), List.of(records.get(), dlqEntries.get()));
  }

  @Test
  void gzippedJsonLinesWithManyShortFields() throws Exception {
    String line = jsonWithTwentyFields();
    long count = TARGET_BYTES / line.length();
    run("JSON_OBJECT_LINE", file("a.jsonl.gz", count, i -> line, true));
    assertEquals(List.of(count, 0L), List.of(records.get(), dlqEntries.get()));
  }

  @Test
  void largeTextRecordsHitTheBatchCap() throws Exception {
    long size = 160 * 1024;
    long count = TARGET_BYTES / size;
    run(
        "STRING_LINE",
        file(
            "a.txt",
            count,
            i -> "x".repeat((int) (32 * 1024 + (i * 7919) % (224 * 1024))) + "\n",
            false));
    assertEquals(List.of(count, 0L), List.of(records.get(), dlqEntries.get()));
    // the 128 KiB batch cap, not the 1,000-record cap, closes these batches of at most 4 records
    assertTrue(batches.get() >= count / 4, "batches: " + batches.get());
  }

  @Test
  void mostlyBadRecordsOfUpTo128KibAreDeadLetteredOneByOne() throws Exception {
    String bad = "not json " + "x".repeat(120 * 1024);
    long count = 1000;
    run(
        "JSON_OBJECT_LINE",
        file("a.jsonl", count, i -> (i % 10 == 0 ? "{\"i\":" + i + "}" : bad) + "\n", false));
    assertEquals(List.of(count / 10, count - count / 10), List.of(records.get(), dlqEntries.get()));
    assertTrue(
        dlqBytes.get() > (count - count / 10) * bad.length(), "every bad record is stored whole");
  }

  private void run(String encodingType, Path file) throws Exception {
    FsSourceCommand command = new FsSourceCommand("source", JobContext.builder().build());
    command.parseAndValidateArg(
        Map.of("backend", "file", "root", root.toUri().toString(), "encodingType", encodingType));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    CountingDlqWriter dlq = new CountingDlqWriter();
    dlq.open();
    ((FsSourceExecutionContext) command.getExecutionContext()).dlqWriter = dlq;
    // Not a mock: Mockito keeps every call's arguments, which would hold every record.
    command.execute(
        "user",
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> batch) {
            records.addAndGet(batch.size());
            batches.incrementAndGet();
          }

          @Override
          public void terminate() {}
        });
    dlq.close();
    assertTrue(Files.exists(file));
  }

  private static String jsonWithTwentyFields() {
    StringBuilder line = new StringBuilder("{");
    for (int f = 0; f < 20; f++) {
      line.append(f == 0 ? "" : ",")
          .append("\"field")
          .append(f)
          .append("\":\"v")
          .append(f)
          .append('"');
    }
    return line.append("}\n").toString();
  }

  private Path file(String name, long count, LongFunction<String> line, boolean gzip)
      throws IOException {
    Path file = root.resolve(name);
    try (OutputStream raw = Files.newOutputStream(file);
        OutputStream out = gzip ? new GZIPOutputStream(raw, 64 * 1024) : raw) {
      for (long i = 0; i < count; i++) {
        out.write(line.apply(i).getBytes(StandardCharsets.UTF_8));
      }
    }
    return file;
  }

  /** A buffered DLQ writer that serializes each batch as S3DlqWriter does, then drops it. */
  private final class CountingDlqWriter extends DlqWriter {
    private final DeadLetterCommiterSerializer serializer = new DeadLetterCommiterSerializer();
    private final BufferedWriter<DeadLetter> buffer =
        new BufferedWriter<>(
            DLQ_BATCH,
            1000,
            batch -> {
              dlqEntries.addAndGet(batch.size());
              dlqBytes.addAndGet(serializer.serialize(batch).length);
            },
            "test-dlq-flusher");

    @Override
    public void open() {
      buffer.start();
    }

    @Override
    protected void doWrite(DeadLetter deadLetter) {
      buffer.write(deadLetter);
    }

    @Override
    public void close() {
      buffer.close();
    }
  }
}
