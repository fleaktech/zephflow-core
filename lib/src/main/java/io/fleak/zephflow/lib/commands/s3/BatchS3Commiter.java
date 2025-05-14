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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.toBase64String;

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Created by bolei on 4/25/25 */
@Slf4j
public class BatchS3Commiter<T> extends S3Commiter<T> {
  private static final int MAX_RETRIES = 3;

  private final int batchSize;
  private final long flushIntervalMillis;
  @VisibleForTesting @Getter private final S3CommiterSerializer<T> serializer;
  private final ScheduledExecutorService scheduler;

  private final List<T> buffer = new ArrayList<>();
  private final Object bufferLock = new Object();
  private ScheduledFuture<?> scheduledFuture;

  public BatchS3Commiter(
      S3Client s3Client,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      S3CommiterSerializer<T> serializer,
      ScheduledExecutorService scheduler) {
    super(s3Client, bucketName);
    this.batchSize = batchSize;
    this.flushIntervalMillis = flushIntervalMillis;
    this.serializer = serializer;
    this.scheduler = scheduler;
  }

  public void open() {
    scheduledFuture =
        scheduler.scheduleAtFixedRate(
            this::commitToS3Scheduled,
            flushIntervalMillis,
            flushIntervalMillis,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public long commit(List<T> events) {
    synchronized (bufferLock) {
      buffer.addAll(events);
      if (buffer.size() >= batchSize) {
        commitToS3Scheduled();
      }
    }
    return 0;
  }

  private void commitToS3Scheduled() {
    commitToS3(System.currentTimeMillis());
  }

  private void commitToS3(long timestamp) {
    List<T> batchToWrite;
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
      data = serializer.serialize(batchToWrite);
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

  @Override
  public void close() {
    scheduledFuture.cancel(false);
    scheduler.shutdown();
    commitToS3Scheduled();
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
