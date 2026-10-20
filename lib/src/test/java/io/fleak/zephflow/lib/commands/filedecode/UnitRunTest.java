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
package io.fleak.zephflow.lib.commands.filedecode;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Counters;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.DownstreamFailed;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.End;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Finished;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Policy;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.ReadFailed;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Settings;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The end-early policy of {@link UnitRun}; the summarizing one is the S3 realtime source's. */
class UnitRunTest {

  private static final String URN = "s3://b/k.json";
  private static final String RENAME = "then upload the file under a new name";
  private static final Map<String, String> URN_METADATA = Map.of("urn", URN);
  private static final DlqTexts TEXTS =
      new DlqTexts("json", false, 0, "file", RENAME, "maxFileBytes", true);

  /** A DLQ entry as a comparable value; the stack trace between the lines varies. */
  record Entry(
      String value,
      Map<String, String> metadata,
      String firstLine,
      String action,
      String lastLine,
      String nodeId) {}

  private static final class TestEmitter implements BatchEmittingConverter.Emitter {
    final List<List<RecordFleakData>> batches = new ArrayList<>();
    RuntimeException failure;

    @Override
    public void accept(List<RecordFleakData> batch) {
      batches.add(List.copyOf(batch));
      if (failure != null) {
        throw failure;
      }
    }

    @Override
    public boolean stopped() {
      return false;
    }
  }

  private static final UnitRun.Unit FILE =
      new UnitRun.Unit() {
        @Override
        public String name() {
          return "file " + URN;
        }

        @Override
        public String location() {
          return URN;
        }

        @Override
        public SerializedEvent recordValue(DecodeOutcome.Failed failed) {
          Map<String, String> metadata = new HashMap<>(URN_METADATA);
          if (failed.recordIndex() >= 1) {
            metadata.put("index", Long.toString(failed.recordIndex()));
          }
          return new SerializedEvent(null, failed.raw(), metadata);
        }

        @Override
        public SerializedEvent summaryValue() {
          return new SerializedEvent(null, new byte[0], URN_METADATA);
        }
      };

  private final List<Entry> dlq = new ArrayList<>();
  private final AtomicLong inputEvents = new AtomicLong();
  private final AtomicLong inputBytes = new AtomicLong();
  private final AtomicLong decodeFailures = new AtomicLong();
  private DlqWriter dlqWriter;
  private TestEmitter emitter;

  @BeforeEach
  void setUp() {
    dlqWriter = mock(DlqWriter.class);
    doAnswer(
            invocation -> {
              SerializedEvent value = invocation.getArgument(1);
              List<String> lines = ((String) invocation.getArgument(2)).lines().toList();
              dlq.add(
                  new Entry(
                      new String(value.value(), StandardCharsets.UTF_8),
                      value.metadata(),
                      lines.getFirst(),
                      lines.get(1).startsWith("Action: ") ? lines.get(1) : null,
                      lines.getLast(),
                      invocation.getArgument(3)));
              return null;
            })
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());
    emitter = new TestEmitter();
  }

  private UnitRun run(DlqWriter writer) {
    return new UnitRun(
        new Settings(TEXTS, Policy.END_EARLY, 4L * 1024 * 1024, writer, "source"),
        new Counters(counter(inputEvents), counter(inputBytes), counter(decodeFailures)),
        FILE,
        emitter);
  }

  @Test
  void aFinishedFileEmitsItsRecordsAndWritesItsEntries() {
    UnitRun run = run(dlqWriter);

    assertTrue(run.visit(decoded(1, 0, 10)));
    assertTrue(run.visit(malformed(2, "{bad", 10, false)));
    assertTrue(run.visit(oversized(3, 14)));
    assertTrue(run.visit(decoded(4, 30, 10)));
    End end = run.complete(100);

    assertEquals(new Finished(), end);
    assertEquals(List.of(List.of(record(1), record(4))), emitter.batches);
    assertEquals(
        List.of(
            new Entry(
                "{bad",
                Map.of("urn", URN, "index", "2"),
                "MALFORMED json record: IOException",
                "Action: fix this record where it is written, or the source's encodingType or"
                    + " decoding if every record fails like this; the value is the record's bytes",
                "file " + URN + ", recordIndex 2, bytes 10-14",
                "source"),
            new Entry(
                "",
                URN_METADATA,
                "OVERSIZED json file: maxRecordBytes",
                "Action: this source sets maxRecordBytes to maxFileBytes, so raise maxFileBytes"
                    + " (and the task's heap to match), or write smaller records; if records should"
                    + " be much smaller, check that the source's format matches the file (a wrong"
                    + " format or framing, or an unclosed csv quote, joins many records into one); "
                    + RENAME,
                "file " + URN + "; 1 outcome; recordIndex 3 (bytes 14-30)",
                "source")),
        dlq);
    assertEquals(
        List.of(2L, 40L, 2L), List.of(inputEvents.get(), inputBytes.get(), decodeFailures.get()));
  }

  @Test
  void aTerminalCompressionFailureEndsTheFileAsAReadFailure() {
    UnitRun run = run(dlqWriter);
    EOFException truncated = new EOFException("unexpected end of gzip data");

    assertTrue(run.visit(decoded(1, 0, 10)));
    assertTrue(run.visit(malformed(2, "{bad", 10, false)));
    assertTrue(run.visit(oversized(3, 14)));
    assertFalse(
        run.visit(
            new DecodeOutcome.Failed(
                ErrorKind.CORRUPT_COMPRESSION, new byte[0], 4, null, truncated, true)));
    assertFalse(run.visit(decoded(5, 40, 10)));

    assertEquals(new ReadFailed(truncated), run.complete(100));
    assertEquals(List.of(), emitter.batches);
    assertEquals(List.of("MALFORMED json record: IOException"), firstLines());
    assertEquals(
        List.of(0L, 0L, 2L), List.of(inputEvents.get(), inputBytes.get(), decodeFailures.get()));
  }

  @Test
  void aReadFailureDropsTheOpenBatchAndTheSummaries() {
    UnitRun run = run(dlqWriter);
    IOException reset = new IOException("connection reset");

    assertTrue(run.visit(decoded(1, 0, 10)));
    assertTrue(run.visit(oversized(2, 10)));
    run.readFailed(reset);

    assertFalse(run.visit(decoded(3, 30, 10)));
    assertEquals(new ReadFailed(reset), run.complete(100));
    assertEquals(List.of(), emitter.batches);
    assertEquals(List.of(), dlq);
  }

  @Test
  void aDownstreamFailureEndsTheFileWithoutItsSummaries() {
    UnitRun run = run(dlqWriter);
    NodeExecutionException failure =
        new NodeExecutionException("eval_1", "eval", "missing field", null);
    emitter.failure = failure;

    assertTrue(run.visit(oversized(1, 0)));
    for (long i = 2; i < UnitRun.MAX_BATCH_RECORDS + 1; i++) {
      assertTrue(run.visit(decoded(i, i * 10, 10)));
    }
    assertFalse(run.visit(decoded(UnitRun.MAX_BATCH_RECORDS + 1, 100_000, 10)));
    assertFalse(run.visit(decoded(UnitRun.MAX_BATCH_RECORDS + 2, 100_010, 10)));

    assertEquals(new DownstreamFailed(failure), run.complete(200_000));
    assertEquals(
        List.of(
            LongStream.rangeClosed(2, UnitRun.MAX_BATCH_RECORDS + 1)
                .mapToObj(UnitRunTest::record)
                .toList()),
        emitter.batches);
    assertEquals(List.of(), dlq);
    assertEquals(
        List.of((long) UnitRun.MAX_BATCH_RECORDS, 100_010L, 1L),
        List.of(inputEvents.get(), inputBytes.get(), decodeFailures.get()));
  }

  @Test
  void aDownstreamFailureOfTheLastBatchEndsTheFileWithoutItsSummaries() {
    UnitRun run = run(dlqWriter);
    NodeExecutionException failure =
        new NodeExecutionException("eval_1", "eval", "missing field", null);
    emitter.failure = failure;

    assertTrue(run.visit(decoded(1, 0, 10)));
    assertTrue(run.visit(oversized(2, 10)));
    assertTrue(run.visit(decoded(3, 26, 10)));

    assertEquals(new DownstreamFailed(failure), run.complete(36));
    assertEquals(List.of(List.of(record(1), record(3))), emitter.batches);
    assertEquals(List.of(), dlq);
    assertEquals(
        List.of(2L, 36L, 1L), List.of(inputEvents.get(), inputBytes.get(), decodeFailures.get()));
  }

  @Test
  void anEarlyEndCountsTheBytesThroughTheLastRecordEmitted() {
    UnitRun run = run(dlqWriter);
    IOException reset = new IOException("connection reset");
    for (long i = 1; i <= UnitRun.MAX_BATCH_RECORDS + 1; i++) {
      assertTrue(run.visit(decoded(i, (i - 1) * 10, 10)));
    }
    run.readFailed(reset);

    assertEquals(new ReadFailed(reset), run.complete(100_000));
    assertEquals(
        List.of((long) UnitRun.MAX_BATCH_RECORDS, UnitRun.MAX_BATCH_RECORDS * 10L),
        List.of(inputEvents.get(), inputBytes.get()));
  }

  @Test
  void theTextsMustMatchThePolicy() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new Settings(TEXTS, Policy.SUMMARIZE, 1024, dlqWriter, "source"));
  }

  @Test
  void aSmallBadDocumentKeepsItsBytesAndOtherTerminalFailuresAreSummarized() {
    UnitRun run = run(dlqWriter);
    run.visit(malformed(-1, "{\"a\":", 0, true));
    run.complete(5);

    UnitRun header = run(dlqWriter);
    header.visit(malformed(0, "a,a", 0, true));
    header.complete(4);

    UnitRun large = run(dlqWriter);
    large.visit(malformed(-1, "x".repeat(UnitRun.MAX_RECORD_ENTRY_BYTES + 1), 0, true));
    large.complete(UnitRun.MAX_RECORD_ENTRY_BYTES + 1);

    String malformedAction =
        "Action: check that the file is json as the source's encodingType or decoding says, or"
            + " fix the listed records where the file is written; "
            + RENAME;
    assertEquals(
        List.of(
            new Entry(
                "{\"a\":",
                URN_METADATA,
                "MALFORMED json record: IOException",
                "Action: fix this record where it is written, or the source's encodingType or"
                    + " decoding if every record fails like this; the value is the record's bytes",
                "file " + URN + ", bytes 0-5",
                "source"),
            new Entry(
                "",
                URN_METADATA,
                "MALFORMED json file: IOException",
                malformedAction,
                "file " + URN + "; 1 outcome; the header (bytes 0-3); the file ended",
                "source"),
            new Entry(
                "",
                URN_METADATA,
                "MALFORMED json file: IOException",
                malformedAction,
                "file "
                    + URN
                    + "; 1 outcome; the document (bytes 0-"
                    + (UnitRun.MAX_RECORD_ENTRY_BYTES + 1)
                    + "); the file ended",
                "source")),
        dlq);
  }

  @Test
  void withoutADlqBadRecordsAreSummarizedAndEachSummaryIsLoggedOnce() {
    try (BoundedLogCapture logs = new BoundedLogCapture(UnitRun.class)) {
      UnitRun run = run(null);
      run.visit(malformed(1, "{bad", 0, false));
      run.visit(malformed(2, "{bad", 5, false));
      run.visit(oversized(3, 10));
      assertEquals(new Finished(), run.complete(30));

      assertEquals(
          List.of(
              List.of(
                  Level.ERROR,
                  "dropping failures of "
                      + URN
                      + " (no DLQ configured): MALFORMED json file:"
                      + " IOException",
                  "file " + URN + "; 2 outcomes; recordIndex 1-2 (bytes 0-9)"),
              List.of(
                  Level.ERROR,
                  "dropping failures of "
                      + URN
                      + " (no DLQ configured): OVERSIZED json file:"
                      + " maxRecordBytes",
                  "file " + URN + "; 1 outcome; recordIndex 3 (bytes 10-26)")),
          logs.events().stream()
              .map(
                  e -> {
                    List<String> lines = e.getMessage().getFormattedMessage().lines().toList();
                    return List.<Object>of(e.getLevel(), lines.getFirst(), lines.getLast());
                  })
              .toList());
    }
    assertEquals(3L, decodeFailures.get());
  }

  @Test
  void anEntryWithTheSummaryValueIsWrittenUnderTheSource() {
    run(dlqWriter).writeEntry("FILE_TOO_LARGE parquet file\nAction: raise it\nfile " + URN);

    assertEquals(
        List.of(
            new Entry(
                "",
                URN_METADATA,
                "FILE_TOO_LARGE parquet file",
                "Action: raise it",
                "file " + URN,
                "source")),
        dlq);
  }

  private List<String> firstLines() {
    return dlq.stream().map(Entry::firstLine).toList();
  }

  private static RecordFleakData record(long index) {
    return (RecordFleakData) FleakData.wrap(Map.of("i", index));
  }

  private static DecodeOutcome decoded(long index, long offset, long length) {
    return new DecodeOutcome.Decoded(record(index), index, new RawSpan(offset, length));
  }

  private static DecodeOutcome malformed(long index, String raw, long offset, boolean terminal) {
    byte[] bytes = raw.getBytes(StandardCharsets.UTF_8);
    return new DecodeOutcome.Failed(
        ErrorKind.MALFORMED,
        bytes,
        index,
        new RawSpan(offset, bytes.length),
        new IOException("bad json"),
        terminal);
  }

  private static DecodeOutcome oversized(long index, long offset) {
    return new DecodeOutcome.Oversized(
        index, 1, new RawSpan(offset, 16), LimitKind.MAX_RECORD_BYTES, 8, 16, new byte[0], false);
  }

  private static FleakCounter counter(AtomicLong total) {
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {
        increase(1, additionalTags);
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        total.addAndGet(n);
      }
    };
  }
}
