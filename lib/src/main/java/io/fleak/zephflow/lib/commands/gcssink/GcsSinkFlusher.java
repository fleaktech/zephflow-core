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
package io.fleak.zephflow.lib.commands.gcssink;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcsSinkFlusher implements SimpleSinkCommand.Flusher<GcsOutboundMessage> {

  private static final DateTimeFormatter OBJECT_NAME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH-mm-ss");

  private final Storage storage;
  private final String bucketName;
  private final String objectPrefix;

  public GcsSinkFlusher(Storage storage, String bucketName, String objectPrefix) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.objectPrefix = objectPrefix;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<GcsOutboundMessage> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<GcsOutboundMessage> messages = preparedInputEvents.preparedList();
    if (messages.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    String combined =
        messages.stream().map(GcsOutboundMessage::content).collect(Collectors.joining("\n"));
    byte[] bytes = combined.getBytes(StandardCharsets.UTF_8);

    String objectName =
        objectPrefix
            + LocalDateTime.now().format(OBJECT_NAME_FORMATTER)
            + "-"
            + UUID.randomUUID()
            + ".jsonl";

    try {
      BlobInfo blobInfo =
          BlobInfo.newBuilder(BlobId.of(bucketName, objectName))
              .setContentType("application/x-ndjson")
              .build();
      storage.create(blobInfo, bytes);

      log.debug("Uploaded {} events to GCS as: {}", messages.size(), objectName);
      return new SimpleSinkCommand.FlushResult(messages.size(), bytes.length, List.of());
    } catch (Exception e) {
      log.error("Failed to upload to GCS: {}", objectName, e);
      List<ErrorOutput> errors =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(p -> new ErrorOutput(p.getLeft(), "GCS upload failed: " + e.getMessage()))
              .collect(Collectors.toList());
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
  }

  @Override
  public void close() {
    // No resources to close
  }
}
