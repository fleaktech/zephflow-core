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
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.s3.BatchS3Commiter;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import java.util.List;
import java.util.concurrent.*;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

/** Created by bolei on 11/8/24 */
@Slf4j
public class S3DlqWriter extends DlqWriter {
  @VisibleForTesting final BatchS3Commiter<DeadLetter> s3Commiter;

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
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "s3-dlq-writer-flusher");
              t.setDaemon(true);
              return t;
            });
    DeadLetterS3CommiterSerializer serializer = new DeadLetterS3CommiterSerializer();
    BatchS3Commiter<DeadLetter> batchS3Commiter =
        new BatchS3Commiter<>(
            s3Client, bucketName, batchSize, flushIntervalMillis, serializer, scheduler);
    return new S3DlqWriter(batchS3Commiter);
  }

  public S3DlqWriter(BatchS3Commiter<DeadLetter> s3Commiter) {
    this.s3Commiter = s3Commiter;
  }

  @Override
  public void open() {
    s3Commiter.open();
  }

  @Override
  protected void doWrite(DeadLetter deadLetter) {
    s3Commiter.commit(List.of(deadLetter));
  }

  @Override
  public void close() {
    s3Commiter.close();
  }
}
