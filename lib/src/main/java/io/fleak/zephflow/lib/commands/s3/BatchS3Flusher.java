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

import static io.fleak.zephflow.lib.utils.MiscUtils.threadSleep;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.AbstractBufferedFlusher;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * Buffered S3 flusher that writes records to temp files using S3FileWriter strategy, then uploads
 * to S3. Supports multiple file formats (text-based and Parquet) through the S3FileWriter
 * interface.
 *
 * <p>Path pattern: {keyName}/year={yyyy}/month={mm}/day={dd}/{uuid}.{ext}
 */
@Slf4j
public class BatchS3Flusher extends AbstractBufferedFlusher<RecordFleakData> {

  private final AwsClientFactory.S3TransferResources s3TransferResources;
  private final String bucketName;
  private final String keyName;
  private final BlobFileWriter<RecordFleakData> fileWriter;
  private final int batchSize;
  private final long flushIntervalMs;
  private Path tempDirectory;

  public BatchS3Flusher(
      AwsClientFactory.S3TransferResources s3TransferResources,
      String bucketName,
      String keyName,
      BlobFileWriter<RecordFleakData> fileWriter,
      int batchSize,
      long flushIntervalMs,
      DlqWriter dlqWriter,
      JobContext jobContext,
      String nodeId) {
    super(dlqWriter, jobContext, nodeId);
    this.s3TransferResources = s3TransferResources;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.fileWriter = fileWriter;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
  }

  @Override
  public void initialize() {
    try {
      tempDirectory = Files.createTempDirectory("s3-batch-flusher");
      tempDirectory.toFile().deleteOnExit();
      log.info("Created temp directory for S3 batch flusher: {}", tempDirectory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory for S3 batch flusher", e);
    }
    super.initialize();
  }

  @Override
  protected int getBatchSize() {
    return batchSize;
  }

  @Override
  protected long getFlushIntervalMs() {
    return flushIntervalMs;
  }

  @Override
  protected String getSchedulerThreadName() {
    return "s3-batch-flusher-timer";
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlush(
      List<Pair<RecordFleakData, RecordFleakData>> batch) throws Exception {
    return doFlushWithRetry(batch);
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlushWithRetry(
      List<Pair<RecordFleakData, RecordFleakData>> batch) throws Exception {
    List<RecordFleakData> records = batch.stream().map(Pair::getRight).toList();

    List<File> tempFiles = fileWriter.writeToTempFiles(records, tempDirectory);
    String baseKey = generateS3KeyBase();

    try {
      return uploadWithRetry(tempFiles, baseKey, records.size());
    } finally {
      for (File tempFile : tempFiles) {
        if (!tempFile.delete()) {
          log.warn("Failed to delete temp file: {}", tempFile);
        }
      }
    }
  }

  private SimpleSinkCommand.FlushResult uploadWithRetry(
      List<File> tempFiles, String baseKey, int recordCount) {
    long retryDelayMs = INITIAL_RETRY_DELAY_MS;
    int attempt = 0;

    while (true) {
      attempt++;
      try {
        long totalSize = 0;
        for (int i = 0; i < tempFiles.size(); i++) {
          File tempFile = tempFiles.get(i);
          String s3Key =
              tempFiles.size() == 1
                  ? baseKey + "." + fileWriter.getFileExtension()
                  : String.format("%s_part%d.%s", baseKey, i, fileWriter.getFileExtension());
          uploadToS3(tempFile, s3Key);
          totalSize += tempFile.length();
        }
        log.info(
            "Uploaded {} records to {} file(s) in s3://{}/...",
            recordCount,
            tempFiles.size(),
            bucketName);
        return new SimpleSinkCommand.FlushResult(recordCount, totalSize, List.of());
      } catch (Exception e) {
        if (attempt >= MAX_WRITE_RETRIES) {
          throw e;
        }
        log.warn(
            "Upload error (attempt {}/{}), retrying in {}ms: {}",
            attempt,
            MAX_WRITE_RETRIES,
            retryDelayMs,
            e.getMessage());
        threadSleep(retryDelayMs);
        retryDelayMs *= 2;
      }
    }
  }

  @Override
  protected void ensureCanWriteRecord(RecordFleakData record) throws Exception {
    fileWriter.validateRecord(record);
  }

  private String generateS3KeyBase() {
    ZonedDateTime utc = ZonedDateTime.now(ZoneOffset.UTC);
    return String.format(
        "%s/year=%04d/month=%02d/day=%02d/%s",
        keyName, utc.getYear(), utc.getMonthValue(), utc.getDayOfMonth(), UUID.randomUUID());
  }

  private void uploadToS3(File file, String s3Key) {
    UploadFileRequest uploadRequest =
        UploadFileRequest.builder()
            .putObjectRequest(b -> b.bucket(bucketName).key(s3Key))
            .source(file.toPath())
            .build();

    FileUpload upload = s3TransferResources.transferManager().uploadFile(uploadRequest);
    upload.completionFuture().join();
  }

  public void close() {
    stopFlushTimer();
    List<Pair<RecordFleakData, RecordFleakData>> remaining = swapBufferIfNotEmpty();
    if (!remaining.isEmpty()) {
      log.info("Flushing {} remaining records on close", remaining.size());
      executeFlush(remaining, java.util.Map.of());
    }
    s3TransferResources.close();
    if (dlqWriter != null) {
      try {
        dlqWriter.close();
      } catch (Exception e) {
        log.warn("Failed to close DLQ writer", e);
      }
    }
    cleanupTempDirectory();
  }

  private void cleanupTempDirectory() {
    if (tempDirectory != null) {
      try {
        FileUtils.deleteDirectory(tempDirectory.toFile());
        log.debug("Cleaned up temp directory: {}", tempDirectory);
      } catch (IOException e) {
        log.warn("Failed to cleanup temp directory: {}", tempDirectory, e);
      }
    }
  }
}
