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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DeadLetterCommiterSerializer;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Drives the S3 realtime converter through {@link SimpleSourceCommand}'s batch path under a 64 MiB
 * heap, with objects far larger than the heap and a real buffered DLQ writer (batch size 10).
 */
@Tag("lowMemory")
class S3RealtimeLowMemoryTest {

  private static final long MIB = 1024L * 1024;
  private static final long TARGET_BYTES = 200 * MIB;
  private static final int DLQ_BATCH = 10;

  @TempDir Path spoolDir;
  @TempDir Path inputs;

  private final AtomicLong records = new AtomicLong();
  private final AtomicLong dlqEntries = new AtomicLong();
  private final AtomicLong dlqBytes = new AtomicLong();

  @BeforeAll
  static void heapIsSmallerThanTheInputs() {
    assertTrue(
        Runtime.getRuntime().maxMemory() < TARGET_BYTES / 2,
        "run through the lowMemoryTest task so the heap is bounded");
  }

  @Test
  void jsonLinesWithManyShortFields() throws IOException {
    String line = jsonWithTwentyFields();
    long count = TARGET_BYTES / line.length();
    run(EncodingType.JSON_OBJECT_LINE, file(count, i -> line, false));
    assertEquals(List.of(count, 0L), List.of(records.get(), dlqEntries.get()));
  }

  @Test
  void gzippedJsonLinesWithManyShortFields() throws IOException {
    String line = jsonWithTwentyFields();
    long count = TARGET_BYTES / line.length();
    run(EncodingType.JSON_OBJECT_LINE, file(count, i -> line, true));
    assertEquals(List.of(count, 0L), List.of(records.get(), dlqEntries.get()));
  }

  @Test
  void largeTextRecordsHitTheBatchCap() throws IOException {
    long size = 160 * 1024;
    long count = TARGET_BYTES / size;
    run(
        EncodingType.STRING_LINE,
        file(count, i -> "x".repeat((int) (32 * 1024 + (i * 7919) % (224 * 1024))) + "\n", false));
    assertEquals(List.of(count, 0L), List.of(records.get(), dlqEntries.get()));
  }

  @Test
  void mostlyBadRecordsOfUpTo128KibAreDeadLetteredOneByOne() throws IOException {
    String bad = "not json " + "x".repeat(120 * 1024);
    long count = 1000;
    run(
        EncodingType.JSON_OBJECT_LINE,
        file(count, i -> (i % 10 == 0 ? "{\"i\":" + i + "}" : bad) + "\n", false));
    assertEquals(List.of(count / 10, count - count / 10), List.of(records.get(), dlqEntries.get()));
    assertTrue(
        dlqBytes.get() > (count - count / 10) * bad.length(), "every bad record is stored whole");
  }

  private void run(EncodingType encodingType, Path object) throws IOException {
    long length = Files.size(object);
    S3Client s3 = mock(S3Client.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenAnswer(
            invocation ->
                new ResponseInputStream<>(
                    GetObjectResponse.builder().contentLength(length).build(),
                    AbortableInputStream.create(
                        new BufferedInputStream(Files.newInputStream(object)))));
    long heap = S3DecodeLimits.nominalHeapBytes();
    S3Decoding decoding = new S3Decoding(encodingType, null, null, false);
    CountingDlqWriter dlq = new CountingDlqWriter();
    dlq.open();
    ConcurrentLinkedQueue<String> confirmed = new ConcurrentLinkedQueue<>();
    S3RealtimeRawDataConverter converter =
        new S3RealtimeRawDataConverter(
            new S3RegionalClientProvider(region -> s3),
            new S3ObjectDecoder(decoding, heap),
            decoding.formatName(),
            Long.MAX_VALUE,
            spoolDir,
            S3DecodeLimits.batchBytes(heap),
            confirmed,
            dlq,
            new S3RealtimeRawDataEncoder(),
            "node",
            noOp());
    S3EventMessage message =
        new S3EventMessage("m", "r", "{}", List.of(new S3ObjectRef("b", "k", "us-east-1")));
    Fetcher<S3EventMessage> fetcher =
        new Fetcher<>() {
          private boolean fetched;

          @Override
          public List<S3EventMessage> fetch() {
            boolean first = !fetched;
            fetched = true;
            return first ? List.of(message) : List.of();
          }

          @Override
          public boolean isExhausted() {
            return fetched;
          }

          @Override
          public void close() {}
        };
    // Not a mock: Mockito keeps every call's arguments, which would hold every record.
    SourceEventAcceptor acceptor =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> batch) {
            records.addAndGet(batch.size());
          }

          @Override
          public void terminate() {}
        };
    Source source =
        new Source(
            new SourceExecutionContext<>(
                fetcher, converter, new S3RealtimeRawDataEncoder(), noOp(), noOp(), noOp(), dlq));
    source.initialize(mock(MetricClientProvider.class));
    source.execute("user", acceptor);
    assertEquals(List.of("r"), List.copyOf(confirmed));
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

  private Path file(long count, LongFunction<String> line, boolean gzip) throws IOException {
    Path file = Files.createTempFile(inputs, "object", gzip ? ".gz" : "");
    try (OutputStream raw = Files.newOutputStream(file);
        OutputStream out = gzip ? new GZIPOutputStream(raw, 64 * 1024) : raw) {
      for (long i = 0; i < count; i++) {
        out.write(line.apply(i).getBytes(StandardCharsets.UTF_8));
      }
    }
    return file;
  }

  private static FleakCounter noOp() {
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {}

      @Override
      public void increase(long n, Map<String, String> additionalTags) {}
    };
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

  private static final class Source extends SimpleSourceCommand<S3EventMessage> {
    private final SourceExecutionContext<S3EventMessage> context;

    Source(SourceExecutionContext<S3EventMessage> context) {
      super("node", mock(JobContext.class), mock(ConfigParser.class), mock(ConfigValidator.class));
      this.context = context;
    }

    @Override
    public String commandName() {
      return "s3-realtime-low-memory";
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    protected SourceExecutionContext<S3EventMessage> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return context;
    }
  }
}
