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
package io.fleak.zephflow.lib.commands.fssource.api;

import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

/** Built-in {@link PostAction} factories. */
public final class PostActions {

  private PostActions() {}

  public static PostAction noOp() {
    return PostAction.NO_OP;
  }

  /** Delete via the backend. Throws if the backend does not advertise DELETE. */
  public static PostAction delete() {
    return (file, backend, cfg) -> {
      if (!backend.capabilities().contains(FsBackend.Capability.DELETE)) {
        throw new UnsupportedOperationException(
            "Backend " + backend.scheme() + " does not support DELETE");
      }
      switch (file.key().backend()) {
        case "file" -> Files.delete(Paths.get(java.net.URI.create(file.key().urn())));
        case "s3" -> {
          S3BackendConfig sc = (S3BackendConfig) cfg;
          String stripped = file.key().urn().substring("s3://".length());
          int slash = stripped.indexOf('/');
          String bucket = stripped.substring(0, slash);
          String objectKey = stripped.substring(slash + 1);
          try (S3Client c = S3Backend.client(sc)) {
            c.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(objectKey).build());
          }
        }
        default ->
            throw new UnsupportedOperationException(
                "Delete not implemented for backend " + file.key().backend() + " in v1");
      }
    };
  }

  /** Move to a sibling prefix. Behavior depends on backend; local FS and S3 are implemented. */
  public static PostAction moveTo(String destinationPrefix) {
    return (file, backend, cfg) -> {
      if (!backend.capabilities().contains(FsBackend.Capability.MOVE)) {
        throw new UnsupportedOperationException(
            "Backend " + backend.scheme() + " does not support MOVE");
      }
      switch (file.key().backend()) {
        case "file" -> {
          Path src = Paths.get(java.net.URI.create(file.key().urn()));
          Path dst =
              Paths.get(
                  java.net.URI.create(destinationPrefix + "/" + src.getFileName().toString()));
          Files.createDirectories(dst.getParent());
          Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
        }
        case "s3" -> {
          S3BackendConfig sc = (S3BackendConfig) cfg;
          String stripped = file.key().urn().substring("s3://".length());
          int slash = stripped.indexOf('/');
          String bucket = stripped.substring(0, slash);
          String srcKey = stripped.substring(slash + 1);
          String filename =
              srcKey.contains("/") ? srcKey.substring(srcKey.lastIndexOf('/') + 1) : srcKey;
          // destinationPrefix is expected as "s3://bucket/prefix"
          String destStripped =
              destinationPrefix.startsWith("s3://")
                  ? destinationPrefix.substring("s3://".length())
                  : destinationPrefix;
          int destSlash = destStripped.indexOf('/');
          String destBucket = destSlash < 0 ? destStripped : destStripped.substring(0, destSlash);
          String destPrefix = destSlash < 0 ? "" : destStripped.substring(destSlash + 1);
          String destKey =
              destPrefix.endsWith("/") ? destPrefix + filename : destPrefix + "/" + filename;
          try (S3Client c = S3Backend.client(sc)) {
            c.copyObject(
                CopyObjectRequest.builder()
                    .sourceBucket(bucket)
                    .sourceKey(srcKey)
                    .destinationBucket(destBucket)
                    .destinationKey(destKey)
                    .build());
            c.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(srcKey).build());
          }
        }
        default ->
            throw new UnsupportedOperationException(
                "MoveTo not implemented for backend " + file.key().backend() + " in v1");
      }
    };
  }
}
