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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public final class PostActions {

  private PostActions() {}

  public static PostAction noOp() {
    return PostAction.NO_OP;
  }

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
        case "gs" -> {
          GcsBackendConfig gc = (GcsBackendConfig) cfg;
          String stripped = file.key().urn().substring("gs://".length());
          int slash = stripped.indexOf('/');
          String bucket = stripped.substring(0, slash);
          String objectKey = stripped.substring(slash + 1);
          Storage gcsClient = GcsBackend.client(gc);
          gcsClient.delete(BlobId.of(bucket, objectKey));
        }
        case "azblob" -> {
          AzureBackendConfig ac = (AzureBackendConfig) cfg;
          URI blobUri = URI.create(file.key().urn());
          String blobPath = blobUri.getPath().substring(1);
          int blobSlash = blobPath.indexOf('/');
          String delContainer = blobPath.substring(0, blobSlash);
          String delBlobName = blobPath.substring(blobSlash + 1);
          AzureBackend.client(ac)
              .getBlobContainerClient(delContainer)
              .getBlobClient(delBlobName)
              .delete();
        }
        default ->
            throw new UnsupportedOperationException(
                "Delete not implemented for backend " + file.key().backend());
      }
    };
  }

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
        case "gs" -> {
          GcsBackendConfig gc = (GcsBackendConfig) cfg;
          String stripped = file.key().urn().substring("gs://".length());
          int slash = stripped.indexOf('/');
          String bucket = stripped.substring(0, slash);
          String srcKey = stripped.substring(slash + 1);
          String filename =
              srcKey.contains("/") ? srcKey.substring(srcKey.lastIndexOf('/') + 1) : srcKey;
          String destStripped =
              destinationPrefix.startsWith("gs://")
                  ? destinationPrefix.substring("gs://".length())
                  : destinationPrefix;
          int destSlash = destStripped.indexOf('/');
          String destBucket = destSlash < 0 ? destStripped : destStripped.substring(0, destSlash);
          String destPrefix = destSlash < 0 ? "" : destStripped.substring(destSlash + 1);
          String destKey =
              destPrefix.endsWith("/") ? destPrefix + filename : destPrefix + "/" + filename;
          Storage gcsClient = GcsBackend.client(gc);
          Storage.CopyRequest copyReq =
              Storage.CopyRequest.newBuilder()
                  .setSource(BlobId.of(bucket, srcKey))
                  .setTarget(BlobInfo.newBuilder(BlobId.of(destBucket, destKey)).build())
                  .build();
          gcsClient.copy(copyReq);
          gcsClient.delete(BlobId.of(bucket, srcKey));
        }
        case "azblob" -> {
          AzureBackendConfig ac = (AzureBackendConfig) cfg;
          BlobServiceClient bsc = AzureBackend.client(ac);

          URI srcUri = URI.create(file.key().urn());
          String srcPath = srcUri.getPath().substring(1);
          int srcSlash = srcPath.indexOf('/');
          String srcContainer = srcPath.substring(0, srcSlash);
          String srcBlob = srcPath.substring(srcSlash + 1);

          URI dstUri = URI.create(destinationPrefix);
          String dstPath = dstUri.getPath().substring(1);
          int dstSlash = dstPath.indexOf('/');
          String dstContainer = dstPath.substring(0, dstSlash);
          String dstPrefix = dstPath.substring(dstSlash + 1);
          String filename =
              srcBlob.contains("/") ? srcBlob.substring(srcBlob.lastIndexOf('/') + 1) : srcBlob;
          String dstBlob =
              dstPrefix.endsWith("/") ? dstPrefix + filename : dstPrefix + "/" + filename;

          BlobClient dstClient = bsc.getBlobContainerClient(dstContainer).getBlobClient(dstBlob);
          dstClient.beginCopy(file.key().urn(), null).waitForCompletion();
          bsc.getBlobContainerClient(srcContainer).getBlobClient(srcBlob).delete();
        }
        default ->
            throw new UnsupportedOperationException(
                "MoveTo not implemented for backend " + file.key().backend());
      }
    };
  }
}
