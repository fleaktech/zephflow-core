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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Copied;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Ended;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Exact;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Opening;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.OverCap;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Downloads one S3 object whole into a file of a spool directory ({@link SpoolDownloader}), with
 * the S3 rules: a missing key, an object over the size limit (not downloaded), and an exact length.
 */
final class S3ObjectDownloader {

  static final int MAX_ATTEMPTS = SpoolDownloader.MAX_ATTEMPTS;

  private final SpoolDownloader downloader;

  S3ObjectDownloader(Path spoolDir) {
    this(spoolDir, Files::newOutputStream);
  }

  S3ObjectDownloader(Path spoolDir, OutputOpener outputOpener) {
    this.downloader = new SpoolDownloader(spoolDir, "object-", outputOpener::open);
  }

  /** Opens the local copy for writing; replaceable so tests can fail the writes. */
  @FunctionalInterface
  interface OutputOpener {
    OutputStream open(Path file) throws IOException;
  }

  sealed interface Download permits Missing, TooLarge, Downloaded {}

  /** The key does not exist (deleted after the notification was sent). */
  record Missing() implements Download {}

  /** The stored size is over the limit; the body was not downloaded. */
  record TooLarge(long storedLength, String eTag, String versionId) implements Download {}

  /** The object's bytes are in {@code file}; closing deletes it. */
  record Downloaded(Path file, long storedLength, String eTag, String versionId)
      implements Download, AutoCloseable {
    @Override
    public void close() {
      SpoolDownloader.deleteQuietly(file);
    }
  }

  Download download(S3Client s3, S3ObjectRef ref, long maxObjectSizeBytes)
      throws SourceReadException {
    GetObjectRequest request =
        GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build();
    String target = "s3://%s/%s".formatted(ref.bucket(), ref.key());
    return switch (downloader.download(
        target, () -> open(s3, request, target, maxObjectSizeBytes))) {
      case Copied<GetObjectResponse> c ->
          new Downloaded(c.file(), c.bytes(), c.info().eTag(), c.info().versionId());
      case Ended<GetObjectResponse>(GetObjectResponse response) ->
          response == null
              ? new Missing()
              : new TooLarge(response.contentLength(), response.eTag(), response.versionId());
      case OverCap<GetObjectResponse> o ->
          throw new IllegalStateException("an exact body has no cap");
    };
  }

  /** The body to download; a missing key or an object over the limit ends without one. */
  private static Opening<GetObjectResponse> open(
      S3Client s3, GetObjectRequest request, String target, long maxObjectSizeBytes)
      throws SourceReadException {
    ResponseInputStream<GetObjectResponse> body;
    try {
      body = s3.getObject(request);
    } catch (NoSuchKeyException e) {
      return new Ended<>(null);
    }
    GetObjectResponse response = body.response();
    Long storedLength = response.contentLength();
    if (storedLength == null) {
      SpoolDownloader.discard(body);
      throw new SourceReadException("opening " + target + " gave no content length", null);
    }
    if (storedLength > maxObjectSizeBytes) {
      SpoolDownloader.discard(body);
      return new Ended<>(response);
    }
    return new Exact<>(body, storedLength, response);
  }
}
