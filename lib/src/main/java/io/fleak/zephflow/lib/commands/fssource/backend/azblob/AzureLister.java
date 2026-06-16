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
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class AzureLister implements FileLister {

  private final BlobServiceClient serviceClient;

  public AzureLister(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    URI uri = URI.create(req.root());
    String host = uri.getHost();
    String path = uri.getPath().substring(1); // strip leading /
    int slash = path.indexOf('/');
    String container = slash < 0 ? path : path.substring(0, slash);
    String prefix = slash < 0 ? "" : path.substring(slash + 1);

    ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
    return StreamSupport.stream(
            serviceClient.getBlobContainerClient(container).listBlobs(options, null).spliterator(),
            false)
        .filter(
            b ->
                req.fileNameRegex() == null
                    || req.fileNameRegex().matcher(filename(b.getName())).matches())
        .map(b -> toEntry(host, container, b));
  }

  @Override
  public FileEntry stat(FileKey key) {
    URI uri = URI.create(key.urn());
    String path = uri.getPath().substring(1);
    int slash = path.indexOf('/');
    String container = path.substring(0, slash);
    String blobName = path.substring(slash + 1);
    var props =
        serviceClient.getBlobContainerClient(container).getBlobClient(blobName).getProperties();
    return new FileEntry(
        key,
        props.getBlobSize(),
        props.getLastModified() == null ? Instant.EPOCH : props.getLastModified().toInstant(),
        key.urn());
  }

  private static FileEntry toEntry(String host, String container, BlobItem b) {
    String urn = "https://" + host + "/" + container + "/" + b.getName();
    return new FileEntry(
        new FileKey(AzureBackend.SCHEME, urn),
        b.getProperties().getContentLength() == null ? 0 : b.getProperties().getContentLength(),
        b.getProperties().getLastModified() == null
            ? Instant.EPOCH
            : b.getProperties().getLastModified().toInstant(),
        urn);
  }

  private static String filename(String name) {
    int i = name.lastIndexOf('/');
    return i < 0 ? name : name.substring(i + 1);
  }
}
