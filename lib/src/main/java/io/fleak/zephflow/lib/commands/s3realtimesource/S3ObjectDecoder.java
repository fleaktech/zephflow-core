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

import static io.fleak.zephflow.lib.commands.s3realtimesource.S3RealtimeRawDataConverter.S3_METADATA_BUCKET;
import static io.fleak.zephflow.lib.commands.s3realtimesource.S3RealtimeRawDataConverter.S3_METADATA_KEY;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Download;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Downloaded;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Missing;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.TooLarge;
import io.fleak.zephflow.lib.serdes.decode.ByteInput;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Decodes one S3 object the way the S3 realtime source does: the decoding inputs of {@link
 * S3Decoding} (with the legacy mapping), the source's fixed limits for a heap size, the download to
 * local disk, and the {@code addS3Metadata} enrichment. Shared by the source and dynamicapi's
 * sampler so a preview matches what a pipeline emits. Compiled once; reusable and thread-safe.
 */
public final class S3ObjectDecoder {

  /** How {@link #decode} ended. */
  public enum Status {
    /** The key does not exist; nothing was decoded. */
    MISSING,
    /** The stored size is over {@code maxObjectSizeBytes}; nothing was downloaded. */
    TOO_LARGE,
    /** Every outcome was handed to the visitor. */
    FINISHED,
    /** The visitor asked to stop. */
    STOPPED
  }

  /**
   * The outcome of {@link #decode}. {@code storedLength}, {@code eTag} and {@code versionId} come
   * from the GetObject response, also for a too large object; -1 and null when it is missing.
   */
  public record Result(Status status, long storedLength, String eTag, String versionId) {}

  /** Receives each outcome; returns false to stop. */
  @FunctionalInterface
  public interface Visitor {
    boolean visit(DecodeOutcome outcome);
  }

  private static final long MIN_HEAP_BYTES = 1024L * 1024;

  private final RecordDecoder recordDecoder;
  private final DecodeLimits limits;
  private final boolean addS3Metadata;

  /**
   * @param heapBytes the heap the limits are proportions of, at least 1 MiB
   * @throws IllegalArgumentException for a configuration that {@link S3Decoding#decodingConfig} or
   *     the decoder rejects, or a heap under 1 MiB
   */
  public S3ObjectDecoder(S3Decoding decoding, long heapBytes) {
    if (heapBytes < MIN_HEAP_BYTES) {
      throw new IllegalArgumentException(
          "heapBytes must be at least " + MIN_HEAP_BYTES + " but was " + heapBytes);
    }
    DecodingConfig config = decoding.decodingConfig();
    this.recordDecoder = DecodingConfigCompiler.compile(config);
    this.limits = S3DecodeLimits.forFormat(config.format(), heapBytes);
    this.addS3Metadata = decoding.addS3Metadata();
  }

  S3ObjectDecoder(RecordDecoder recordDecoder, DecodeLimits limits, boolean addS3Metadata) {
    this.recordDecoder = recordDecoder;
    this.limits = limits;
    this.addS3Metadata = addS3Metadata;
  }

  /** The compiled decoder, for a caller that holds an object's bytes itself. */
  public RecordDecoder recordDecoder() {
    return recordDecoder;
  }

  /** The limits the decoder is opened with. */
  public DecodeLimits limits() {
    return limits;
  }

  /**
   * Downloads the object into {@code spoolDir}, which the caller owns, hands each outcome to the
   * visitor until it asks to stop or the object ends, and deletes the local file.
   *
   * @throws SourceReadException when the download fails, or reading the local copy does
   */
  public Result decode(
      S3Client s3, S3ObjectRef ref, long maxObjectSizeBytes, Path spoolDir, Visitor visitor)
      throws SourceReadException {
    Download download = new S3ObjectDownloader(spoolDir).download(s3, ref, maxObjectSizeBytes);
    return switch (download) {
      case Missing m -> new Result(Status.MISSING, -1, null, null);
      case TooLarge t -> new Result(Status.TOO_LARGE, t.storedLength(), t.eTag(), t.versionId());
      case Downloaded d -> decode(d, ref, visitor);
    };
  }

  private Result decode(Downloaded downloaded, S3ObjectRef ref, Visitor visitor)
      throws SourceReadException {
    Status status = Status.FINISHED;
    try (downloaded;
        RecordCursor cursor = recordDecoder.open(ByteInput.ofFile(downloaded.file()), limits)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        if (!visitor.visit(enrich(outcome, ref))) {
          status = Status.STOPPED;
          break;
        }
      }
    }
    return new Result(status, downloaded.storedLength(), downloaded.eTag(), downloaded.versionId());
  }

  private DecodeOutcome enrich(DecodeOutcome outcome, S3ObjectRef ref) {
    if (!addS3Metadata || !(outcome instanceof DecodeOutcome.Decoded decoded)) {
      return outcome;
    }
    Map<String, FleakData> payload = new HashMap<>(decoded.record().getPayload());
    payload.put(S3_METADATA_BUCKET, FleakData.wrap(ref.bucket()));
    payload.put(S3_METADATA_KEY, FleakData.wrap(ref.key()));
    return new DecodeOutcome.Decoded(
        new RecordFleakData(payload), decoded.recordIndex(), decoded.span());
  }
}
