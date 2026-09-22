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

  /** A bucket and an object key, split out of an {@code s3://bucket/key} urn. */
  record S3Location(String bucket, String key) {

    static S3Location ofObject(String urn) {
      String stripped = urn.substring("s3://".length());
      int slash = stripped.indexOf('/');
      if (slash < 0 || slash == stripped.length() - 1) {
        throw new IllegalArgumentException("s3 urn names no object key: " + urn);
      }
      return new S3Location(stripped.substring(0, slash), stripped.substring(slash + 1));
    }

    /**
     * Splits a root urn, normalizing a non-empty prefix to end with {@code /} so it matches a
     * folder rather than every sibling key that merely starts with the same characters.
     */
    static S3Location ofRoot(String urn) {
      String stripped = urn.substring("s3://".length());
      int slash = stripped.indexOf('/');
      if (slash < 0) {
        return new S3Location(stripped, "");
      }
      String prefix = stripped.substring(slash + 1);
      if (!prefix.isEmpty() && !prefix.endsWith("/")) {
        prefix = prefix + "/";
      }
      return new S3Location(stripped.substring(0, slash), prefix);
    }
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    S3Location root = S3Location.ofRoot(req.root());
    String listPrefix = req.exactObjectKey() == null ? root.key() : req.exactObjectKey();

    var pages =
        client.listObjectsV2Paginator(
            ListObjectsV2Request.builder().bucket(root.bucket()).prefix(listPrefix).build());
    return StreamSupport.stream(pages.contents().spliterator(), false)
        .filter(
            s3Object -> req.exactObjectKey() == null || req.exactObjectKey().equals(s3Object.key()))
        .filter(
            s3Object ->
                req.fileNameRegex() == null
                    || req.fileNameRegex().matcher(filename(s3Object.key())).matches())
        .map(s3Object -> toEntry(root.bucket(), s3Object));
  }

  private static String filename(String key) {
    int slashIndex = key.lastIndexOf('/');
    return slashIndex < 0 ? key : key.substring(slashIndex + 1);
  }

  private static FileEntry toEntry(String bucket, S3Object s3Object) {
    String urn = "s3://" + bucket + "/" + s3Object.key();
    return new FileEntry(
        new FileKey("s3", urn),
        s3Object.size(),
        Instant.ofEpochMilli(s3Object.lastModified().toEpochMilli()),
        urn);
  }

  @Override
  public FileEntry stat(FileKey key) {
    S3Location location = S3Location.ofObject(key.urn());
    HeadObjectResponse headResponse =
        client.headObject(
            HeadObjectRequest.builder().bucket(location.bucket()).key(location.key()).build());
    return new FileEntry(
        key,
        headResponse.contentLength(),
        Instant.ofEpochMilli(headResponse.lastModified().toEpochMilli()),
        key.urn());
  }

  @Override
  public void close() {
    client.close();
  }
}
