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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.toBase64String;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Created by bolei on 11/8/24 */
@Slf4j
public class S3DlqWriter extends DlqWriter {

  private static final int MAX_RETRIES = 3;

  @VisibleForTesting @Getter private final S3Client s3Client;
  private final String bucketName;

  private final int batchSize;
  private final long flushIntervalMillis;
  private final List<DeadLetter> buffer;
  private final ScheduledExecutorService scheduler;

  private final Object bufferLock = new Object();

  private ScheduledFuture<?> scheduledFuture;

  public static S3DlqWriter createS3DlqWriter(JobContext.S3DlqConfig s3DlqConfig) {
    return createS3DlqWriter(
        s3DlqConfig.getRegion(),
        s3DlqConfig.getBucket(),
        s3DlqConfig.getBatchSize(),
        s3DlqConfig.getFlushIntervalMillis(),
        null,
        null);
  }

  @VisibleForTesting
  public static S3DlqWriter createS3DlqWriter(
      @Nonnull String region,
      @NonNull String bucketName,
      int batchSize,
      long flushIntervalMillis,
      UsernamePasswordCredential credential,
      String s3EndpointOverride) {
    Preconditions.checkArgument(
        batchSize > 0, "batchSize must be positive but provided %d", batchSize);
    Preconditions.checkArgument(
        flushIntervalMillis > 0,
        "flushIntervalMillis must be positive but provided %d",
        flushIntervalMillis);

    S3Client s3Client =
        new AwsClientFactory().createS3Client(region, credential, s3EndpointOverride);
    // Initialize the scheduler for periodic flushing
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    List<DeadLetter> buffer = new ArrayList<>();
    return new S3DlqWriter(s3Client, bucketName, batchSize, flushIntervalMillis, buffer, scheduler);
  }

  private S3DlqWriter(
      S3Client s3Client,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      List<DeadLetter> buffer,
      ScheduledExecutorService scheduler) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.batchSize = batchSize;
    this.flushIntervalMillis = flushIntervalMillis;
    this.buffer = buffer;
    this.scheduler = scheduler;
  }

  @Override
  public void open() {
    scheduledFuture =
        scheduler.scheduleAtFixedRate(
            this::flushScheduled, flushIntervalMillis, flushIntervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void doWrite(DeadLetter deadLetter) {
    synchronized (bufferLock) {
      buffer.add(deadLetter);
      if (buffer.size() >= batchSize) {
        flush(deadLetter.getProcessingTimestamp());
      }
    }
  }

  private void flushScheduled() {
    flush(System.currentTimeMillis());
  }

  private void flush(long timestamp) {
    List<DeadLetter> batchToWrite;
    synchronized (bufferLock) {
      if (buffer.isEmpty()) {
        return;
      }
      // Create a copy of the buffer to write and clear the original buffer
      batchToWrite = new ArrayList<>(buffer);
      buffer.clear();
    }

    byte[] data;
    try {
      data = serializeDeadLetters(batchToWrite);
    } catch (Exception e) {
      log.error("failed to serialize dead letters: {}", toJsonString(batchToWrite), e);
      return;
    }
    String objectKey = generateS3ObjectKey(timestamp);
    try {
      uploadToS3(data, objectKey);
    } catch (Exception e) {
      log.error("failed to write to DQL. data: {}", toBase64String(data), e);
    }
  }

  private void uploadToS3(byte[] data, String objectKey) {
    int attempt = 0;
    while (true) {
      try {

        PutObjectRequest putObjectRequest =
            PutObjectRequest.builder().bucket(bucketName).key(objectKey).build();

        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));
        return; // Success
      } catch (Exception e) {
        if (attempt >= MAX_RETRIES) {
          throw e; // Re-throw after max retries
        }
        // Wait before retrying
        MiscUtils.threadSleep(1000L * (attempt + 1));
      }
      attempt++;
    }
  }

  private byte[] serializeDeadLetters(List<DeadLetter> deadLetters) throws IOException {
    // Use Avro DataFileWriter for serialization
    SpecificDatumWriter<DeadLetter> datumWriter = new SpecificDatumWriter<>(DeadLetter.class);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (DataFileWriter<DeadLetter> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(deadLetters.getFirst().getSchema(), outputStream);
      for (DeadLetter deadLetter : deadLetters) {
        dataFileWriter.append(deadLetter);
      }
    }
    return outputStream.toByteArray();
  }

  private String generateS3ObjectKey(long timestamp) {
    Instant instant = Instant.ofEpochMilli(timestamp);

    ZonedDateTime utcDateTime = instant.atZone(ZoneOffset.UTC);

    String year = String.format("%04d", utcDateTime.getYear());
    String month = String.format("%02d", utcDateTime.getMonthValue());
    String day = String.format("%02d", utcDateTime.getDayOfMonth());
    String hour = String.format("%02d", utcDateTime.getHour());

    // Get the epoch milliseconds for the file name
    // Construct the S3 object key
    return String.format("%s/%s/%s/%s/dead-letters-%d.avro", year, month, day, hour, timestamp);
  }

  @Override
  public void close() {
    scheduledFuture.cancel(false);
    scheduler.shutdown();
    flush(System.currentTimeMillis());
    boolean terminated = false;
    for (int i = 0; i < MAX_RETRIES; ++i) {
      try {
        terminated = scheduler.awaitTermination(5, TimeUnit.SECONDS);
        if (terminated) {
          break;
        }
      } catch (InterruptedException e) {
        // no-op
      }
    }
    if (!terminated) {
      log.error("failed to terminate scheduler after {}} attempts", MAX_RETRIES);
    }
  }
}
