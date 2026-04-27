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
package io.fleak.zephflow.lib.commands.azureblobsource;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class AzureBlobSourceFetcher implements Fetcher<AzureBlobData> {

  private static final int FETCH_BATCH_SIZE = 100;

  private final BlobContainerClient containerClient;
  private final String blobPrefix;

  private Iterator<BlobItem> blobIterator;
  private boolean initialized = false;
  private boolean exhausted = false;

  public AzureBlobSourceFetcher(BlobContainerClient containerClient, String blobPrefix) {
    this.containerClient = containerClient;
    this.blobPrefix = blobPrefix;
  }

  @Override
  public List<AzureBlobData> fetch() {
    if (!initialized) {
      ListBlobsOptions options = new ListBlobsOptions();
      if (StringUtils.isNotBlank(blobPrefix)) {
        options.setPrefix(blobPrefix);
      }
      blobIterator = containerClient.listBlobs(options, null).iterator();
      initialized = true;
      log.info(
          "Initialized Azure Blob fetcher for container: {}",
          containerClient.getBlobContainerName());
    }

    if (exhausted || !blobIterator.hasNext()) {
      exhausted = true;
      return List.of();
    }

    List<AzureBlobData> results = new ArrayList<>();
    int count = 0;
    while (blobIterator.hasNext() && count < FETCH_BATCH_SIZE) {
      BlobItem blobItem = blobIterator.next();
      String blobName = blobItem.getName();
      try {
        byte[] content = containerClient.getBlobClient(blobName).downloadContent().toBytes();
        Map<String, String> metadata =
            blobItem.getMetadata() != null ? blobItem.getMetadata() : Map.of();
        results.add(new AzureBlobData(content, blobName, metadata));
        count++;
      } catch (Exception e) {
        log.error("Failed to download blob: {}", blobName, e);
      }
    }

    if (!blobIterator.hasNext()) {
      exhausted = true;
    }

    log.debug("Fetched {} blobs from Azure Blob Storage", results.size());
    return results;
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    // No resources to close
  }
}
