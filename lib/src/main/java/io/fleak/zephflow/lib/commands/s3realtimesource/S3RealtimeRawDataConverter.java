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
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Downloads the S3 object(s) referenced by an {@link S3EventMessage} — one at a time, so at most a
 * single object is held in memory — and deserializes each into {@link RecordFleakData}. On success
 * the message's receipt handle is queued for commit (deletion); on failure the whole message is
 * routed to the DLQ and left undeleted for SQS redelivery.
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

  public S3RealtimeRawDataConverter(
      S3Client s3Client,
      FleakDeserializer<?> fleakDeserializer,
      CompressionType compressionType,
      long maxObjectSizeBytes,
      boolean addS3Metadata,
      Queue<String> confirmedReceiptHandles) {
    this.s3Client = s3Client;
    this.fleakDeserializer = fleakDeserializer;
    this.compressionType = compressionType;
    this.maxObjectSizeBytes = maxObjectSizeBytes;
    this.addS3Metadata = addS3Metadata;
    this.confirmedReceiptHandles = confirmedReceiptHandles;
  }

  @Override
  public ConvertedResult<S3EventMessage> convert(
      S3EventMessage sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      List<RecordFleakData> events = new ArrayList<>();
      long totalBytes = 0;
      for (S3ObjectRef ref : sourceRecord.objectRefs()) {
        byte[] raw;
        try {
          raw = downloadObject(ref);
        } catch (NoSuchKeyException e) {
          // The object was deleted (e.g. by an S3 lifecycle policy) between the notification being
          // emitted and us reading it. It will never appear, so acknowledge and skip rather than
          // retrying. Common with backlogged queues on log buckets that expire objects.
          log.warn(
              "S3 object s3://{}/{} no longer exists; skipping the notification",
              ref.bucket(),
              ref.key());
          continue;
        }
        byte[] body = maybeDecompress(raw);
        totalBytes += body.length;
        List<RecordFleakData> objectEvents =
            fleakDeserializer.deserialize(new SerializedEvent(null, body, null));
        if (addS3Metadata) {
          objectEvents.forEach(e -> enrich(e, ref));
        }
        events.addAll(objectEvents);
      }

      Map<String, String> eventTags =
          getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());
      sourceInitializedConfig.dataSizeCounter().increase(totalBytes, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);

      confirmedReceiptHandles.add(sourceRecord.receiptHandle());
      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("failed to process S3 event message {}", sourceRecord.messageId(), e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }

  private byte[] downloadObject(S3ObjectRef ref) throws IOException {
    try (ResponseInputStream<GetObjectResponse> in =
        s3Client.getObject(
            GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build())) {
      long contentLength = in.response().contentLength();
      if (contentLength > maxObjectSizeBytes) {
        throw new IOException(
            "S3 object s3://%s/%s size %d exceeds maxObjectSizeBytes %d"
                .formatted(ref.bucket(), ref.key(), contentLength, maxObjectSizeBytes));
      }
      return in.readAllBytes();
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
