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
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public final class S3Lister implements FileLister {

  private final S3Client client;

  public S3Lister(S3Client client) {
    this.client = client;
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    // Parse "s3://bucket/prefix/" from req.root().
    String urn = req.root();
    String stripped = urn.substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = slash < 0 ? stripped : stripped.substring(0, slash);
    String prefix = slash < 0 ? "" : stripped.substring(slash + 1);

    var iter =
        client.listObjectsV2Paginator(
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());
    return StreamSupport.stream(iter.contents().spliterator(), false)
        .filter(
            o ->
                req.fileNameRegex() == null
                    || req.fileNameRegex().matcher(filename(o.key())).matches())
        .map(o -> toEntry(bucket, o));
  }

  private static String filename(String key) {
    int i = key.lastIndexOf('/');
    return i < 0 ? key : key.substring(i + 1);
  }

  private static FileEntry toEntry(String bucket, S3Object o) {
    String urn = "s3://" + bucket + "/" + o.key();
    return new FileEntry(
        new FileKey("s3", urn),
        o.size(),
        Instant.ofEpochMilli(o.lastModified().toEpochMilli()),
        urn);
  }

  @Override
  public FileEntry stat(FileKey key) {
    // s3://bucket/key
    String stripped = key.urn().substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    HeadObjectResponse h =
        client.headObject(HeadObjectRequest.builder().bucket(bucket).key(objectKey).build());
    return new FileEntry(
        key, h.contentLength(), Instant.ofEpochMilli(h.lastModified().toEpochMilli()), key.urn());
  }

  @Override
  public void close() {
    client.close();
  }
}
