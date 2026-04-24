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
package io.fleak.zephflow.lib.commands.azureblobsink;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureBlobSinkFlusher implements SimpleSinkCommand.Flusher<AzureBlobOutboundMessage> {

  private static final DateTimeFormatter BLOB_NAME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH-mm-ss");

  private final BlobContainerClient containerClient;
  private final String blobNamePrefix;

  public AzureBlobSinkFlusher(BlobContainerClient containerClient, String blobNamePrefix) {
    this.containerClient = containerClient;
    this.blobNamePrefix = blobNamePrefix;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<AzureBlobOutboundMessage> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<AzureBlobOutboundMessage> messages = preparedInputEvents.preparedList();
    if (messages.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    String combined =
        messages.stream().map(AzureBlobOutboundMessage::content).collect(Collectors.joining("\n"));
    byte[] bytes = combined.getBytes(StandardCharsets.UTF_8);

    String blobName =
        blobNamePrefix
            + ZonedDateTime.now(ZoneOffset.UTC).format(BLOB_NAME_FORMATTER)
            + "-"
            + UUID.randomUUID()
            + ".jsonl";

    try {
      BlobClient blobClient = containerClient.getBlobClient(blobName);
      blobClient.upload(BinaryData.fromBytes(bytes), true);

      log.debug("Uploaded {} events to Azure Blob Storage as: {}", messages.size(), blobName);
      return new SimpleSinkCommand.FlushResult(messages.size(), bytes.length, List.of());
    } catch (Exception e) {
      log.error("Failed to upload blob: {}", blobName, e);
      List<ErrorOutput> errors =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(p -> new ErrorOutput(p.getLeft(), "Azure Blob upload failed: " + e.getMessage()))
              .collect(Collectors.toList());
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
  }

  @Override
  public void close() {
    // No resources to close
  }
}
