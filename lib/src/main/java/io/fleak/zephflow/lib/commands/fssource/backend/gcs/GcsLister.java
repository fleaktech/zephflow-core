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
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class GcsLister implements FileLister {

  private final Storage storage;

  public GcsLister(Storage storage) {
    this.storage = storage;
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    String urn = req.root();
    String stripped = urn.substring("gs://".length());
    int slash = stripped.indexOf('/');
    String bucket = slash < 0 ? stripped : stripped.substring(0, slash);
    String prefix = slash < 0 ? "" : stripped.substring(slash + 1);
    Page<Blob> page = storage.list(bucket, BlobListOption.prefix(prefix));
    return StreamSupport.stream(page.iterateAll().spliterator(), false)
        .filter(
            b ->
                req.fileNameRegex() == null
                    || req.fileNameRegex().matcher(filename(b.getName())).matches())
        .map(
            b -> {
              String fullUrn = "gs://" + bucket + "/" + b.getName();
              return new FileEntry(
                  new FileKey("gs", fullUrn),
                  b.getSize() == null ? 0 : b.getSize(),
                  b.getUpdateTimeOffsetDateTime() == null
                      ? Instant.EPOCH
                      : b.getUpdateTimeOffsetDateTime().toInstant(),
                  fullUrn);
            });
  }

  private static String filename(String key) {
    int i = key.lastIndexOf('/');
    return i < 0 ? key : key.substring(i + 1);
  }

  @Override
  public FileEntry stat(FileKey key) {
    String stripped = key.urn().substring("gs://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    Blob b = storage.get(bucket, objectKey);
    return new FileEntry(
        key,
        b.getSize() == null ? 0 : b.getSize(),
        b.getUpdateTimeOffsetDateTime() == null
            ? Instant.EPOCH
            : b.getUpdateTimeOffsetDateTime().toInstant(),
        key.urn());
  }
}
