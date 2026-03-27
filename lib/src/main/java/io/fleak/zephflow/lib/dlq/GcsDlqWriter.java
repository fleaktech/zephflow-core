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
package io.fleak.zephflow.lib.dlq;

import static io.fleak.zephflow.lib.utils.MiscUtils.threadSleep;
import static io.fleak.zephflow.lib.utils.MiscUtils.toBase64String;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcsDlqWriter extends DlqWriter {
  private static final int MAX_RETRIES = 3;
  private static final String DEFAULT_PATH_SEGMENT = "dead-letters";
  private static final String DEFAULT_FILE_PREFIX = "deadletter";

  @VisibleForTesting final BufferedWriter<DeadLetter> bufferedWriter;
  @VisibleForTesting final Storage storage;
  private final String bucketName;
  private final String keyPrefix;
  private final String pathSegment;
  private final String filePrefix;
  private final DeadLetterCommiterSerializer serializer;

  public static GcsDlqWriter createGcsDlqWriter(
      JobContext.GcsDlqConfig gcsDlqConfig, String keyPrefix) {
    return createWriter(
        gcsDlqConfig.getServiceAccountJson(),
        gcsDlqConfig.getBucket(),
        gcsDlqConfig.getBatchSize(),
        gcsDlqConfig.getFlushIntervalMillis(),
        keyPrefix,
        DEFAULT_PATH_SEGMENT,
        DEFAULT_FILE_PREFIX);
  }

  public static GcsDlqWriter createGcsSampleWriter(
      JobContext.GcsDlqConfig gcsDlqConfig, String keyPrefix) {
    return createWriter(
        gcsDlqConfig.getServiceAccountJson(),
        gcsDlqConfig.getBucket(),
        gcsDlqConfig.getBatchSize(),
        gcsDlqConfig.getFlushIntervalMillis(),
        keyPrefix,
        "raw-data-samples",
        "sample");
  }

  private static GcsDlqWriter createWriter(
      String serviceAccountJson,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      String keyPrefix,
      String pathSegment,
      String filePrefix) {
    Preconditions.checkArgument(
        batchSize > 0, "batchSize must be positive but provided %d", batchSize);
    Preconditions.checkArgument(
        flushIntervalMillis > 0,
        "flushIntervalMillis must be positive but provided %d",
        flushIntervalMillis);
    Storage storage = new GcsClientFactory().createStorageClient(serviceAccountJson);
    return new GcsDlqWriter(
        storage, bucketName, batchSize, flushIntervalMillis, keyPrefix, pathSegment, filePrefix);
  }

  @VisibleForTesting
  GcsDlqWriter(
      Storage storage,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      String keyPrefix,
      String pathSegment,
      String filePrefix) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.keyPrefix = sanitizeKeyPrefix(keyPrefix);
    this.pathSegment = pathSegment;
    this.filePrefix = filePrefix;
    this.serializer = new DeadLetterCommiterSerializer();
    this.bufferedWriter =
        new BufferedWriter<>(
            batchSize, flushIntervalMillis, this::uploadToGcs, "gcs-" + pathSegment + "-flusher");
  }

  @Override
  public void open() {
    bufferedWriter.start();
  }

  @Override
  protected void doWrite(DeadLetter deadLetter) {
    bufferedWriter.write(deadLetter);
  }

  @Override
  public void close() {
    bufferedWriter.close();
    try {
      storage.close();
    } catch (Exception e) {
      log.warn("Failed to close GCS storage client", e);
    }
  }

  private static String sanitizeKeyPrefix(String prefix) {
    if (prefix == null) return null;
    String sanitized = prefix.strip();
    while (sanitized.startsWith("/")) sanitized = sanitized.substring(1);
    while (sanitized.endsWith("/")) sanitized = sanitized.substring(0, sanitized.length() - 1);
    return sanitized.isEmpty() ? null : sanitized;
  }

  private void uploadToGcs(List<DeadLetter> batch) {
    long timestamp = System.currentTimeMillis();

    byte[] data;
    try {
      data = serializer.serialize(batch);
    } catch (Exception e) {
      log.error("failed to serialize {}, dropping {} records", pathSegment, batch.size(), e);
      return;
    }

    String objectName = generateObjectKey(timestamp);
    try {
      uploadToGcsWithRetry(data, objectName);
      log.info("Uploaded {} records to gs://{}/{}", batch.size(), bucketName, objectName);
    } catch (Exception e) {
      log.error("failed to write to GCS {}. data: {}", pathSegment, toBase64String(data), e);
    }
  }

  @VisibleForTesting
  String generateObjectKey(long timestamp) {
    Instant instant = Instant.ofEpochMilli(timestamp);
    ZonedDateTime utcDateTime = instant.atZone(ZoneOffset.UTC);

    String date =
        String.format(
            "%04d-%02d-%02d",
            utcDateTime.getYear(), utcDateTime.getMonthValue(), utcDateTime.getDayOfMonth());
    String hour = String.format("%02d", utcDateTime.getHour());
    String uuid = UUID.randomUUID().toString();

    String timePath =
        String.format(
            "%s/dt=%s/hr=%s/%s-%d_%s.avro", pathSegment, date, hour, filePrefix, timestamp, uuid);
    if (keyPrefix != null && !keyPrefix.isEmpty()) {
      return keyPrefix + "/" + timePath;
    }
    return timePath;
  }

  private void uploadToGcsWithRetry(byte[] data, String objectName) {
    int attempt = 0;
    while (true) {
      try {
        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName).build();
        storage.create(blobInfo, data);
        return;
      } catch (Exception e) {
        if (attempt >= MAX_RETRIES) {
          throw e;
        }
        log.warn(
            "GCS upload failed (attempt {}/{}), retrying: {}",
            attempt + 1,
            MAX_RETRIES,
            e.getMessage());
        threadSleep(1000L * (attempt + 1));
      }
      attempt++;
    }
  }
}
