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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobRange;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.InputStream;
import java.net.URI;

public final class AzureReader implements FileReader {

  private final BlobServiceClient serviceClient;

  public AzureReader(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  @Override
  public InputStream open(FileKey key, long offset) {
    URI uri = URI.create(key.urn());
    String path = uri.getPath().substring(1); // strip leading /
    int slash = path.indexOf('/');
    String container = path.substring(0, slash);
    String blobName = path.substring(slash + 1);

    BlobRange range = new BlobRange(offset);
    return serviceClient
        .getBlobContainerClient(container)
        .getBlobClient(blobName)
        .openInputStream(range, null);
  }
}
