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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Slf4j
public class S3DlqWriter extends DlqWriter {
  private static final int MAX_RETRIES = 3;
  private static final String DEFAULT_PATH_SEGMENT = "dead-letters";
  private static final String DEFAULT_FILE_PREFIX = "deadletter";

  @VisibleForTesting final BufferedWriter<DeadLetter> bufferedWriter;
  @VisibleForTesting final S3Client s3Client;
  private final String bucketName;
  private final String keyPrefix;
  private final String pathSegment;
  private final String filePrefix;
  private final DeadLetterS3CommiterSerializer serializer;

  public static S3DlqWriter createS3DlqWriter(
      JobContext.S3DlqConfig s3DlqConfig, String keyPrefix) {
    return createWriter(
        s3DlqConfig.getRegion(),
        s3DlqConfig.getBucket(),
        s3DlqConfig.getBatchSize(),
        s3DlqConfig.getFlushIntervalMillis(),
        new UsernamePasswordCredential(
            s3DlqConfig.getAccessKeyId(), s3DlqConfig.getSecretAccessKey()),
        s3DlqConfig.getS3EndpointOverride(),
        keyPrefix,
        DEFAULT_PATH_SEGMENT,
        DEFAULT_FILE_PREFIX);
  }

  @VisibleForTesting
  public static S3DlqWriter createS3DlqWriter(
      @Nonnull String region,
      @NonNull String bucketName,
      int batchSize,
      long flushIntervalMillis,
      UsernamePasswordCredential credential,
      String s3EndpointOverride,
      String keyPrefix) {
    return createWriter(
        region,
        bucketName,
        batchSize,
        flushIntervalMillis,
        credential,
        s3EndpointOverride,
        keyPrefix,
        DEFAULT_PATH_SEGMENT,
        DEFAULT_FILE_PREFIX);
  }

  public static S3DlqWriter createS3SampleWriter(
      JobContext.S3DlqConfig s3DlqConfig, String keyPrefix) {
    return createWriter(
        s3DlqConfig.getRegion(),
        s3DlqConfig.getBucket(),
        s3DlqConfig.getBatchSize(),
        s3DlqConfig.getFlushIntervalMillis(),
        new UsernamePasswordCredential(
            s3DlqConfig.getAccessKeyId(), s3DlqConfig.getSecretAccessKey()),
        s3DlqConfig.getS3EndpointOverride(),
        keyPrefix,
        "raw-data-samples",
        "sample");
  }

  @VisibleForTesting
  public static S3DlqWriter createS3SampleWriter(
      @Nonnull String region,
      @NonNull String bucketName,
      int batchSize,
      long flushIntervalMillis,
      UsernamePasswordCredential credential,
      String s3EndpointOverride,
      String keyPrefix) {
    return createWriter(
        region,
        bucketName,
        batchSize,
        flushIntervalMillis,
        credential,
        s3EndpointOverride,
        keyPrefix,
        "raw-data-samples",
        "sample");
  }

  private static S3DlqWriter createWriter(
      @Nonnull String region,
      @NonNull String bucketName,
      int batchSize,
      long flushIntervalMillis,
      UsernamePasswordCredential credential,
      String s3EndpointOverride,
      String keyPrefix,
      String pathSegment,
      String filePrefix) {
    Preconditions.checkArgument(
        batchSize > 0, "batchSize must be positive but provided %d", batchSize);
    Preconditions.checkArgument(
        flushIntervalMillis > 0,
        "flushIntervalMillis must be positive but provided %d",
        flushIntervalMillis);
    S3Client s3Client =
        new AwsClientFactory().createS3Client(region, credential, s3EndpointOverride);
    return new S3DlqWriter(
        s3Client, bucketName, batchSize, flushIntervalMillis, keyPrefix, pathSegment, filePrefix);
  }

  @VisibleForTesting
  S3DlqWriter(
      S3Client s3Client,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      String keyPrefix,
      String pathSegment,
      String filePrefix) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.keyPrefix = sanitizeKeyPrefix(keyPrefix);
    this.pathSegment = pathSegment;
    this.filePrefix = filePrefix;
    this.serializer = new DeadLetterS3CommiterSerializer();
    this.bufferedWriter =
        new BufferedWriter<>(
            batchSize, flushIntervalMillis, this::uploadToS3, "s3-" + pathSegment + "-flusher");
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
    s3Client.close();
  }

  private static String sanitizeKeyPrefix(String prefix) {
    if (prefix == null) return null;
    String sanitized = prefix.strip();
    while (sanitized.startsWith("/")) sanitized = sanitized.substring(1);
    while (sanitized.endsWith("/")) sanitized = sanitized.substring(0, sanitized.length() - 1);
    return sanitized.isEmpty() ? null : sanitized;
  }

  private void uploadToS3(List<DeadLetter> batch) {
    long timestamp = System.currentTimeMillis();

    byte[] data;
    try {
      data = serializer.serialize(batch);
    } catch (Exception e) {
      log.error("failed to serialize {}, dropping {} records", pathSegment, batch.size(), e);
      return;
    }

    String objectKey = generateS3ObjectKey(timestamp);
    try {
      uploadToS3WithRetry(data, objectKey);
      log.info("Uploaded {} records to s3://{}/{}", batch.size(), bucketName, objectKey);
    } catch (Exception e) {
      log.error("failed to write to S3 {}. data: {}", pathSegment, toBase64String(data), e);
    }
  }

  @VisibleForTesting
  String generateS3ObjectKey(long timestamp) {
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

  private void uploadToS3WithRetry(byte[] data, String objectKey) {
    int attempt = 0;
    while (true) {
      try {
        PutObjectRequest putObjectRequest =
            PutObjectRequest.builder().bucket(bucketName).key(objectKey).build();
        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));
        return;
      } catch (Exception e) {
        if (attempt >= MAX_RETRIES) {
          throw e;
        }
        log.warn(
            "S3 upload failed (attempt {}/{}), retrying: {}",
            attempt + 1,
            MAX_RETRIES,
            e.getMessage());
        threadSleep(1000L * (attempt + 1));
      }
      attempt++;
    }
  }
}
