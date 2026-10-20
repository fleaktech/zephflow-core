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

import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_FS_SOURCE_URN;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Counters;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter.Emitter;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

/** A file whose format needs random access and that does not fit in the spool. */
class FsFileDecoderTest {

  private static final String URN = "gs://b/data.parquet";
  private static final long CAP = 4;

  record Entry(String value, Map<String, String> metadata, List<String> lines, String nodeId) {}

  private final List<Entry> dlq = new ArrayList<>();

  @Test
  void aParquetFileOverTheSpoolCapIsTooLargeDeadLetteredAndLogged() {
    FsFileDecoder decoder =
        new FsFileDecoder(
            new SourceDecoding(
                null, null, new DecodingConfig(List.of(), new ParquetFormatConfig())),
            null,
            false,
            64L * 1024 * 1024,
            CAP,
            new CapturingDlqWriter(),
            "source",
            new Counters(noOp(), noOp(), noOp()));
    FsFileAccess access =
        new FsFileAccess(
            (key, offset) ->
                new ByteArrayInputStream("PAR1".repeat(25).getBytes(StandardCharsets.UTF_8)),
            false,
            CAP);

    try (BoundedLogCapture logs = new BoundedLogCapture(FsFileDecoder.class);
        access) {
      FsFileDecoder.Result result =
          decoder.decode(
              new FileEntry(FileKey.of(URN), 100, Instant.EPOCH, "data.parquet"),
              access,
              emitter());

      assertEquals(FsFileDecoder.Result.TOO_LARGE, result);
      assertEquals(
          List.of(
              "fs_source drop file urn="
                  + URN
                  + " reason=file_too_large listedSizeBytes=100: a parquet file needs random access"
                  + " and is over localSpoolMaxBytes 4, so it is not read. It is checkpointed as"
                  + " done and will not be retried; raise localSpoolMaxBytes above its size and"
                  + " upload it under a new name"),
          logs.events().stream()
              .filter(e -> e.getLevel() == Level.ERROR)
              .map(e -> e.getMessage().getFormattedMessage())
              .toList());
    }
    assertEquals(
        List.of(
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, URN),
                List.of(
                    "FILE_TOO_LARGE parquet file",
                    "Action: raise localSpoolMaxBytes above the file's size, with that much free"
                        + " disk where the task runs; then upload the corrected file under a new"
                        + " name; a name already read is not read again",
                    "file " + URN + " (listed size 100) does not fit in localSpoolMaxBytes 4"),
                "source")),
        dlq);
  }

  private static Emitter emitter() {
    return new Emitter() {
      @Override
      public void accept(List<RecordFleakData> batch) {
        throw new AssertionError("nothing is emitted");
      }

      @Override
      public boolean stopped() {
        return false;
      }
    };
  }

  private static FleakCounter noOp() {
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {}

      @Override
      public void increase(long n, Map<String, String> additionalTags) {}
    };
  }

  private class CapturingDlqWriter extends DlqWriter {
    @Override
    protected void doWrite(DeadLetter deadLetter) {
      Map<String, String> metadata = new HashMap<>();
      deadLetter.getMetadata().forEach((k, v) -> metadata.put(k.toString(), v.toString()));
      dlq.add(
          new Entry(
              StandardCharsets.UTF_8.decode(deadLetter.getValue().duplicate()).toString(),
              metadata,
              deadLetter.getErrorMessage().lines().toList(),
              deadLetter.getNodeId()));
    }

    @Override
    public void open() {}

    @Override
    public void close() {}
  }
}
