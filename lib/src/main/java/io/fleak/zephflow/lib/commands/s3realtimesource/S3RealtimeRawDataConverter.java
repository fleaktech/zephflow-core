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

import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.CompressionUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Downloads the S3 object(s) referenced by an {@link S3EventMessage} — one at a time, so at most a
 * single object is held in memory — and deserializes each into {@link RecordFleakData}.
 *
 * <p>Each object is handled independently and a failure is <b>terminal</b>: an object that can't be
 * downloaded or parsed is written (with its real error) to the DLQ and skipped, never retried. So
 * {@code convert()} always succeeds and the message is always acknowledged; a multi-object message
 * emits its good objects and DLQs the bad ones. ({@code NoSuchKey} / oversized are benign skips
 * with no DLQ.) Downstream {@code accept()} failures are handled by the framework's DLQ path.
 */
@Slf4j
public class S3RealtimeRawDataConverter implements RawDataConverter<S3EventMessage> {

  static final String S3_METADATA_BUCKET = "__s3_bucket";
  static final String S3_METADATA_KEY = "__s3_key";

  private final S3Client s3Client;
  private final FleakDeserializer<?> fleakDeserializer;
  // When null, gzip is auto-detected via magic bytes; when GZIP, decompression is forced.
  private final CompressionType compressionType;
  private final long maxObjectSizeBytes;
  private final boolean addS3Metadata;
  private final Queue<String> confirmedReceiptHandles;
  private final DlqWriter dlqWriter;
  private final RawDataEncoder<S3EventMessage> encoder;
  private final String nodeId;

  public S3RealtimeRawDataConverter(
      S3Client s3Client,
      FleakDeserializer<?> fleakDeserializer,
      CompressionType compressionType,
      long maxObjectSizeBytes,
      boolean addS3Metadata,
      Queue<String> confirmedReceiptHandles,
      DlqWriter dlqWriter,
      RawDataEncoder<S3EventMessage> encoder,
      String nodeId) {
    this.s3Client = s3Client;
    this.fleakDeserializer = fleakDeserializer;
    this.compressionType = compressionType;
    this.maxObjectSizeBytes = maxObjectSizeBytes;
    this.addS3Metadata = addS3Metadata;
    this.confirmedReceiptHandles = confirmedReceiptHandles;
    this.dlqWriter = dlqWriter;
    this.encoder = encoder;
    this.nodeId = nodeId;
  }

  @Override
  public ConvertedResult<S3EventMessage> convert(
      S3EventMessage sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    List<RecordFleakData> events = new ArrayList<>();
    long totalBytes = 0;
    for (S3ObjectRef ref : sourceRecord.objectRefs()) {
      byte[] body;
      try {
        body = maybeDecompress(downloadObject(ref));
      } catch (NoSuchKeyException e) {
        // The object was deleted (e.g. by an S3 lifecycle policy) between the notification being
        // emitted and us reading it. It will never appear, so acknowledge and skip (no DLQ).
        // Common with backlogged queues on log buckets that expire objects.
        log.warn(
            "S3 object s3://{}/{} no longer exists; skipping the notification",
            ref.bucket(),
            ref.key());
        continue;
      } catch (ObjectTooLargeException e) {
        // Terminal but benign (the object will always be too large): skip + acknowledge, no DLQ.
        log.warn("{}; skipping the notification", e.getMessage());
        continue;
      } catch (Exception e) {
        // Any other download problem is treated as terminal: dead-letter (with the real error) and
        // skip, rather than failing the whole message and retrying.
        deadLetter(sourceRecord, ref, e, sourceInitializedConfig);
        continue;
      }

      try {
        List<RecordFleakData> objectEvents =
            fleakDeserializer.deserialize(new SerializedEvent(null, body, null));
        if (addS3Metadata) {
          objectEvents.forEach(e -> enrich(e, ref));
        }
        events.addAll(objectEvents);
        totalBytes += body.length;
      } catch (Exception e) {
        // The object's content can't be parsed; retrying won't help -> dead-letter + skip.
        deadLetter(sourceRecord, ref, e, sourceInitializedConfig);
      }
    }

    Map<String, String> eventTags =
        getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());
    sourceInitializedConfig.dataSizeCounter().increase(totalBytes, eventTags);
    sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);

    confirmedReceiptHandles.add(sourceRecord.receiptHandle());
    return ConvertedResult.success(events, sourceRecord);
  }

  private void deadLetter(
      S3EventMessage message, S3ObjectRef ref, Exception e, SourceExecutionContext<?> ctx) {
    ctx.deserializeFailureCounter().increase(Map.of());
    log.error(
        "failed to process s3://{}/{} from message {}",
        ref.bucket(),
        ref.key(),
        message.messageId(),
        e);
    if (dlqWriter == null) {
      log.warn("dropping failed object s3://{}/{} (no DLQ configured)", ref.bucket(), ref.key());
      return;
    }
    String errorMsg =
        "failed to process s3://%s/%s: %s"
            .formatted(ref.bucket(), ref.key(), ExceptionUtils.getStackTrace(e));
    try {
      dlqWriter.writeToDlq(
          System.currentTimeMillis(), encoder.serialize(message), errorMsg, nodeId);
    } catch (Exception ex) {
      log.error("failed to write failed object s3://{}/{} to DLQ", ref.bucket(), ref.key(), ex);
    }
  }

  private byte[] downloadObject(S3ObjectRef ref) throws IOException {
    try (ResponseInputStream<GetObjectResponse> in =
        s3Client.getObject(
            GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build())) {
      long contentLength = in.response().contentLength();
      if (contentLength > maxObjectSizeBytes) {
        // Read the size from the response headers and abort before transferring the body.
        throw new ObjectTooLargeException(
            "S3 object s3://%s/%s size %d exceeds maxObjectSizeBytes %d"
                .formatted(ref.bucket(), ref.key(), contentLength, maxObjectSizeBytes));
      }
      return in.readAllBytes();
    }
  }

  private static final class ObjectTooLargeException extends RuntimeException {
    ObjectTooLargeException(String message) {
      super(message);
    }
  }

  private byte[] maybeDecompress(byte[] body) {
    if (compressionType == CompressionType.GZIP || (compressionType == null && isGzip(body))) {
      return CompressionUtils.gunzip(body);
    }
    return body;
  }

  private static boolean isGzip(byte[] body) {
    return body.length >= 2 && (body[0] & 0xFF) == 0x1F && (body[1] & 0xFF) == 0x8B;
  }

  private static void enrich(RecordFleakData event, S3ObjectRef ref) {
    event.getPayload().put(S3_METADATA_BUCKET, FleakData.wrap(ref.bucket()));
    event.getPayload().put(S3_METADATA_KEY, FleakData.wrap(ref.key()));
  }
}
