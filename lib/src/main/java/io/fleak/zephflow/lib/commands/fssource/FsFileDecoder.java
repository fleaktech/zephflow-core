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

import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_FS_SOURCE_RECORD_INDEX;
import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_FS_SOURCE_URN;

import io.fleak.zephflow.lib.commands.filedecode.DlqTexts;
import io.fleak.zephflow.lib.commands.filedecode.HeapDecodeLimits;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Counters;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.DownstreamFailed;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Finished;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Policy;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.ReadFailed;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Settings;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun.Stopped;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.Copied;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.FileBytes;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.InPlace;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.Streamed;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.TooLarge;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter.Emitter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.ByteInput;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Decodes one listed file with the generic decoder and emits its records in bounded batches ({@link
 * UnitRun}, end-early policy): a read or downstream failure ends the file without its summaries, so
 * it is read again next run.
 */
@Slf4j
final class FsFileDecoder {

  /** How a file ended, for the run's checkpoint and counters. */
  enum Result {
    /** Every outcome was handled; the file is done. */
    DONE,
    /** It needs random access and is over localSpoolMaxBytes; it is not read and is done. */
    TOO_LARGE,
    READ_ERROR,
    DOWNSTREAM_ERROR
  }

  private final RecordDecoder decoder;
  private final DecodeLimits limits;
  private final boolean randomAccess;
  private final String format;
  private final String retry;
  private final Settings settings;
  private final Counters counters;
  private final long localSpoolMaxBytes;

  FsFileDecoder(
      SourceDecoding decoding,
      Long maxFileBytes,
      boolean fileNameRegex,
      boolean exactObjectKey,
      long heapBytes,
      long localSpoolMaxBytes,
      DlqWriter dlqWriter,
      String nodeId,
      Counters counters) {
    DecodingConfig config = decoding.decodingConfig();
    this.decoder = DecodingConfigCompiler.compile(config);
    this.limits = HeapDecodeLimits.forFormat(config.format(), heapBytes, maxFileBytes);
    this.randomAccess = config.format() instanceof ParquetFormatConfig;
    this.format = SourceDecoding.formatName(config.format());
    this.retry =
        exactObjectKey
            ? "then upload the corrected file under a new key and set exactObjectKey to it; a key"
                + " already read is not read again"
            : fileNameRegex
                ? "then upload the corrected file under a new name that matches fileNameRegex (with a"
                    + " ts no older than the files already read, if the regex has one); a name already"
                    + " read is not read again"
                : "then upload the corrected file under a new name; a name already read is not read"
                    + " again";
    this.settings =
        new Settings(
            new DlqTexts(
                format,
                SourceDecoding.multiline(config.format()),
                heapBytes,
                "file",
                retry,
                maxFileBytes == null ? null : "maxFileBytes",
                true),
            Policy.END_EARLY,
            HeapDecodeLimits.batchBytes(heapBytes),
            dlqWriter,
            nodeId);
    this.counters = counters;
    this.localSpoolMaxBytes = localSpoolMaxBytes;
  }

  Result decode(FileEntry entry, FsFileAccess access, Emitter emitter) {
    String urn = entry.key().urn();
    FileBytes bytes;
    try {
      bytes = access.open(entry, randomAccess);
    } catch (Exception e) {
      logReadFailure(urn, e, false);
      return Result.READ_ERROR;
    }
    try (bytes) {
      ByteInput input =
          switch (bytes) {
            case InPlace inPlace -> inPlace.input();
            case Copied copied -> copied.input();
            case Streamed streamed -> streamed.input();
            case TooLarge tooLarge -> {
              tooLarge(entry, emitter);
              yield null;
            }
          };
      if (input == null) {
        return Result.TOO_LARGE;
      }
      UnitRun run = new UnitRun(settings, counters, new FileUnit(urn), emitter);
      try (RecordCursor cursor = decoder.open(input, limits)) {
        DecodeOutcome outcome;
        while ((outcome = cursor.next()) != null && run.visit(outcome)) {}
      } catch (Exception e) {
        run.readFailed(e);
      }
      return switch (run.complete(bytes instanceof Copied copied ? copied.bytes() : entry.size())) {
        case Finished finished -> Result.DONE;
        case Stopped stopped -> throw new IllegalStateException("FsSource's emitter never stops");
        case ReadFailed failed -> {
          logReadFailure(urn, failed.error(), failed.compression());
          yield Result.READ_ERROR;
        }
        case DownstreamFailed failed -> {
          log.error("fs_source skip file urn={} due to downstream error", urn, failed.error());
          yield Result.DOWNSTREAM_ERROR;
        }
      };
    }
  }

  private void tooLarge(FileEntry entry, Emitter emitter) {
    String urn = entry.key().urn();
    log.error(
        "fs_source drop file urn={} reason={} listedSizeBytes={}: a {} file needs random access"
            + " and is over localSpoolMaxBytes {}, so it is not read. It is checkpointed as done and"
            + " will not be retried; raise localSpoolMaxBytes above its size and upload it under a"
            + " new name",
        urn,
        FsSourceCommand.SKIP_REASON_FILE_TOO_LARGE,
        entry.size(),
        format,
        localSpoolMaxBytes);
    if (settings.dlqWriter() != null) {
      new UnitRun(settings, counters, new FileUnit(urn), emitter)
          .writeEntry(
              "FILE_TOO_LARGE "
                  + format
                  + " file\nAction: raise localSpoolMaxBytes above the file's size, with that much"
                  + " free disk where the task runs; "
                  + retry
                  + "\nfile "
                  + urn
                  + " (listed size "
                  + entry.size()
                  + ") does not fit in localSpoolMaxBytes "
                  + localSpoolMaxBytes);
    }
  }

  private static void logReadFailure(String urn, Exception error, boolean compression) {
    log.error(
        "fs_source skip file urn={} due to a read failure; it is read again on every run until it"
            + " is fixed. Check {}",
        urn,
        compression
            ? "whether the file was still being written when it was listed, the compression"
                + " setting (decoding.compression, or gzip detected automatically for"
                + " encodingType), and whether its compressed data is damaged or has bytes after"
                + " its end, in which case rewrite the file"
            : "the backend's read permission, the network, and free disk where the task runs",
        error);
  }

  /** A file: its urn in texts, its record bytes and urn as the DLQ values. */
  private record FileUnit(String urn) implements UnitRun.Unit {
    @Override
    public String name() {
      return "file " + urn;
    }

    @Override
    public String location() {
      return urn;
    }

    @Override
    public SerializedEvent recordValue(DecodeOutcome.Failed failed) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(METADATA_FS_SOURCE_URN, urn);
      if (failed.recordIndex() >= 1) {
        metadata.put(METADATA_FS_SOURCE_RECORD_INDEX, Long.toString(failed.recordIndex()));
      }
      return new SerializedEvent(null, failed.raw(), metadata);
    }

    @Override
    public SerializedEvent summaryValue() {
      return new SerializedEvent(null, new byte[0], Map.of(METADATA_FS_SOURCE_URN, urn));
    }
  }
}
