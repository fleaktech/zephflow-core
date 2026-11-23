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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider.NoopMetricClientProvider.NoopFleakCounter;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Slf4j
public class S3DlqWriter extends DlqWriter {
  private static final String DEFAULT_PATH_SEGMENT = "dead-letters";
  private static final String DEFAULT_FILE_PREFIX = "deadletter";

  /**
   * How a batch is uploaded. Against AWS the writer keeps its 4 attempts on top of the SDK's own
   * retries. Against an S3-compatible store the SDK does not retry, so the writer's 3 attempts are
   * the only ones, and each attempt may take {@link #STORAGE_ATTEMPT_BASE_TIMEOUT} plus its payload
   * at {@link #STORAGE_MINIMUM_BYTES_PER_SECOND}.
   */
  record UploadPolicy(int maxAttempts, boolean scalesAttemptTimeoutWithPayload) {}

  static final UploadPolicy AWS_UPLOADS = new UploadPolicy(4, false);
  static final UploadPolicy STORAGE_UPLOADS = new UploadPolicy(3, true);

  /**
   * Covers connecting and the store's answer to a small PUT. With the payload term, a 10 MB batch
   * gets 25 s per attempt, so a store that never answers costs it 3 × 25 s plus 1 s and 2 s of
   * backoff = 78 s; only larger batches get proportionally more.
   */
  static final Duration STORAGE_ATTEMPT_BASE_TIMEOUT = Duration.ofSeconds(15);

  /** The slowest link an upload is still expected to finish on, about an 8 Mbit/s VPN. */
  static final long STORAGE_MINIMUM_BYTES_PER_SECOND = 1_000_000;

  /**
   * Time {@link #close()} gives buffered dead letters to reach the store; what has not been written
   * by then is counted as not written. It matches the 30 s the buffer waits for its flusher thread.
   */
  static final Duration CLOSE_DEADLINE = Duration.ofSeconds(30);

  /**
   * Timeouts of the S3-compatible store's HTTP client. A batch is flushed on the pipeline's own
   * thread once it is full, so a store that hangs must fail the upload instead of stalling the
   * pipeline. The connect timeout is 5 s, above the Apache client's 2 s default, because a
   * customer's store may sit behind a VPN or in another region: 5 s survives one lost SYN, which
   * Linux retransmits after 1 s and again 2 s later. A read waits for the store's answer and keeps
   * the SDK's 30 s; the attempt timeout of each PUT bounds the whole exchange.
   */
  static final AwsClientFactory.S3ClientTimeouts STORAGE_CLIENT_TIMEOUTS =
      new AwsClientFactory.S3ClientTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(30));

  @VisibleForTesting final BufferedWriter<DeadLetter> bufferedWriter;
  @VisibleForTesting final S3Client s3Client;
  private final String bucketName;
  private final String keyPrefix;
  private final String pathSegment;
  private final String filePrefix;
  private final DeadLetterCommiterSerializer serializer;
  private final FleakCounter unwrittenDeadLetterCounter;
  private final UploadPolicy uploadPolicy;
  private volatile long closeDeadlineNanos;
  private volatile boolean closing;

  public static S3DlqWriter createS3DlqWriter(
      JobContext.S3DlqConfig s3DlqConfig,
      String keyPrefix,
      FleakCounter unwrittenDeadLetterCounter) {
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
        DEFAULT_FILE_PREFIX,
        unwrittenDeadLetterCounter);
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
        DEFAULT_FILE_PREFIX,
        new NoopFleakCounter());
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
        "sample",
        new NoopFleakCounter());
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
        "sample",
        new NoopFleakCounter());
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
      String filePrefix,
      FleakCounter unwrittenDeadLetterCounter) {
    Preconditions.checkArgument(
        batchSize > 0, "batchSize must be positive but provided %d", batchSize);
    Preconditions.checkArgument(
        flushIntervalMillis > 0,
        "flushIntervalMillis must be positive but provided %d",
        flushIntervalMillis);
    AwsClientFactory awsClientFactory = new AwsClientFactory();
    boolean awsStorage = StringUtils.isBlank(s3EndpointOverride);
    S3Client s3Client =
        awsStorage
            ? awsClientFactory.createS3Client(region, credential, s3EndpointOverride)
            : awsClientFactory.createS3CompatibleStorageClient(
                region, credential, s3EndpointOverride, STORAGE_CLIENT_TIMEOUTS);
    return new S3DlqWriter(
        s3Client,
        bucketName,
        batchSize,
        flushIntervalMillis,
        keyPrefix,
        pathSegment,
        filePrefix,
        unwrittenDeadLetterCounter,
        awsStorage ? AWS_UPLOADS : STORAGE_UPLOADS);
  }

  @VisibleForTesting
  S3DlqWriter(
      S3Client s3Client,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      String keyPrefix,
      String pathSegment,
      String filePrefix,
      FleakCounter unwrittenDeadLetterCounter,
      UploadPolicy uploadPolicy) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.keyPrefix = sanitizeKeyPrefix(keyPrefix);
    this.pathSegment = pathSegment;
    this.filePrefix = filePrefix;
    this.serializer = new DeadLetterCommiterSerializer();
    this.unwrittenDeadLetterCounter = unwrittenDeadLetterCounter;
    this.uploadPolicy = uploadPolicy;
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
    closeDeadlineNanos = System.nanoTime() + CLOSE_DEADLINE.toNanos();
    closing = true;
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

  @Override
  public void writeNow(List<DeadLetter> deadLetters) throws IOException {
    byte[] data = serializer.serialize(deadLetters);
    String objectKey = generateS3ObjectKey(System.currentTimeMillis(), deadLetters.size());
    uploadToS3WithRetry(data, objectKey);
    log.info("Uploaded {} records to s3://{}/{}", deadLetters.size(), bucketName, objectKey);
  }

  private void uploadToS3(List<DeadLetter> batch) {
    long timestamp = System.currentTimeMillis();

    byte[] data;
    try {
      data = serializer.serialize(batch);
    } catch (Exception e) {
      unwrittenDeadLetterCounter.increase(batch.size(), Map.of());
      log.error(
          "Dropped {} {} records, serialization failed: {}",
          batch.size(),
          pathSegment,
          describeFailure(e));
      return;
    }

    String objectKey = generateS3ObjectKey(timestamp, batch.size());
    try {
      uploadToS3WithRetry(data, objectKey);
      log.info("Uploaded {} records to s3://{}/{}", batch.size(), bucketName, objectKey);
    } catch (Exception e) {
      unwrittenDeadLetterCounter.increase(batch.size(), Map.of());
      log.error(
          "Dropped {} {} records, object {} not written: {}",
          batch.size(),
          pathSegment,
          objectKey,
          e instanceof UploadAbandonedException ? e.getMessage() : describeFailure(e));
    }
  }

  @VisibleForTesting
  String generateS3ObjectKey(long timestamp, int recordCount) {
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
            "%s/dt=%s/hr=%s/%s-%d_%s_n%d.avro",
            pathSegment, date, hour, filePrefix, timestamp, uuid, recordCount);
    if (keyPrefix != null && !keyPrefix.isEmpty()) {
      return keyPrefix + "/" + timePath;
    }
    return timePath;
  }

  private void uploadToS3WithRetry(byte[] data, String objectKey) {
    Exception lastFailure = null;
    for (int attempt = 1; ; attempt++) {
      if (Thread.currentThread().isInterrupted()) {
        throw new UploadAbandonedException("interrupted", lastFailure);
      }
      Duration timeLeft = timeLeftBeforeCloseDeadline();
      if (timeLeft != null && !timeLeft.isPositive()) {
        throw new UploadAbandonedException("the writer's close deadline passed", lastFailure);
      }
      try {
        s3Client.putObject(
            putRequest(objectKey, data.length, timeLeft), RequestBody.fromBytes(data));
        return;
      } catch (Exception e) {
        if (attempt >= uploadPolicy.maxAttempts()) {
          throw e;
        }
        lastFailure = e;
        log.warn(
            "Upload of {} failed (attempt {}/{}), retrying: {}",
            objectKey,
            attempt,
            uploadPolicy.maxAttempts(),
            describeFailure(e));
        if (!sleepBeforeRetry(Duration.ofSeconds(attempt))) {
          throw new UploadAbandonedException("interrupted", e);
        }
      }
    }
  }

  private PutObjectRequest putRequest(String objectKey, int payloadBytes, Duration timeLeft) {
    PutObjectRequest.Builder request = PutObjectRequest.builder().bucket(bucketName).key(objectKey);
    if (uploadPolicy.scalesAttemptTimeoutWithPayload() || timeLeft != null) {
      request.overrideConfiguration(
          override -> {
            if (uploadPolicy.scalesAttemptTimeoutWithPayload()) {
              override.apiCallAttemptTimeout(storageAttemptTimeout(payloadBytes));
            }
            if (timeLeft != null) {
              override.apiCallTimeout(timeLeft);
            }
          });
    }
    return request.build();
  }

  static Duration storageAttemptTimeout(long payloadBytes) {
    return STORAGE_ATTEMPT_BASE_TIMEOUT.plusMillis(
        payloadBytes * 1000 / STORAGE_MINIMUM_BYTES_PER_SECOND);
  }

  /** Null until {@link #close()} starts. */
  private Duration timeLeftBeforeCloseDeadline() {
    return closing ? Duration.ofNanos(closeDeadlineNanos - System.nanoTime()) : null;
  }

  /** Unlike {@code MiscUtils.threadSleep}, keeps the interrupt so the caller can stop. */
  private static boolean sleepBeforeRetry(Duration backoff) {
    try {
      Thread.sleep(backoff.toMillis());
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** The writer stopped retrying for a reason of its own; the message names it. */
  private static final class UploadAbandonedException extends RuntimeException {
    UploadAbandonedException(String reason, Exception lastFailure) {
      super(lastFailure == null ? reason : reason + " after " + describeFailure(lastFailure));
    }
  }
}
