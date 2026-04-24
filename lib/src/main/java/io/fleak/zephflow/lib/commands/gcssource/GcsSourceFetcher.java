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
package io.fleak.zephflow.lib.commands.gcssource;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
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
public class GcsSourceFetcher implements Fetcher<GcsObjectData> {

  private static final int FETCH_BATCH_SIZE = 100;

  private final Storage storage;
  private final String bucketName;
  private final String objectPrefix;

  private Iterator<Blob> blobIterator;
  private boolean initialized = false;
  private boolean exhausted = false;

  public GcsSourceFetcher(Storage storage, String bucketName, String objectPrefix) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.objectPrefix = objectPrefix;
  }

  @Override
  public List<GcsObjectData> fetch() {
    if (!initialized) {
      Storage.BlobListOption[] options =
          StringUtils.isNotBlank(objectPrefix)
              ? new Storage.BlobListOption[] {Storage.BlobListOption.prefix(objectPrefix)}
              : new Storage.BlobListOption[0];
      blobIterator = storage.list(bucketName, options).iterateAll().iterator();
      initialized = true;
      log.info("Initialized GCS source fetcher for bucket: {}", bucketName);
    }

    if (exhausted || !blobIterator.hasNext()) {
      exhausted = true;
      return List.of();
    }

    List<GcsObjectData> results = new ArrayList<>();
    int count = 0;
    while (blobIterator.hasNext() && count < FETCH_BATCH_SIZE) {
      Blob blob = blobIterator.next();
      String objectName = blob.getName();
      try {
        byte[] content = storage.readAllBytes(BlobId.of(bucketName, objectName));
        Map<String, String> metadata = blob.getMetadata() != null ? blob.getMetadata() : Map.of();
        results.add(new GcsObjectData(content, objectName, metadata));
        count++;
      } catch (Exception e) {
        log.error("Failed to download GCS object: {}", objectName, e);
      }
    }

    if (!blobIterator.hasNext()) {
      exhausted = true;
    }

    log.debug("Fetched {} objects from GCS bucket: {}", results.size(), bucketName);
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
