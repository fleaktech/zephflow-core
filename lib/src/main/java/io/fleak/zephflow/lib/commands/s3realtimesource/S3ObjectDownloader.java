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

import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Downloads one S3 object whole into a file of a spool directory, so that nothing is decoded while
 * the network is involved. A body that breaks mid-download is downloaded again from the beginning;
 * opening errors, local write failures and interrupts are not retried.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
final class S3ObjectDownloader {

  static final int MAX_ATTEMPTS = 3;
  private static final int BUFFER_SIZE = 64 * 1024;

  private final Path spoolDir;
  private final OutputOpener outputOpener;

  S3ObjectDownloader(Path spoolDir) {
    this(spoolDir, Files::newOutputStream);
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
  record TooLarge(long storedLength) implements Download {}

  /** The object's bytes are in {@code file}; closing deletes it. */
  record Downloaded(Path file, long storedLength, String eTag, String versionId)
      implements Download, AutoCloseable {
    @Override
    public void close() {
      deleteQuietly(file);
    }
  }

  Download download(S3Client s3, S3ObjectRef ref, long maxObjectSizeBytes)
      throws SourceReadException {
    GetObjectRequest request =
        GetObjectRequest.builder().bucket(ref.bucket()).key(ref.key()).build();
    List<Exception> failures = new ArrayList<>();
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      ResponseInputStream<GetObjectResponse> body;
      try {
        body = s3.getObject(request);
      } catch (NoSuchKeyException e) {
        return new Missing();
      } catch (RuntimeException e) {
        throw new SourceReadException(describe(ref, "opening") + " failed", e);
      }
      GetObjectResponse response = body.response();
      Long storedLength = response.contentLength();
      if (storedLength == null || storedLength > maxObjectSizeBytes) {
        abortQuietly(body);
        closeQuietly(body);
        if (storedLength == null) {
          throw new SourceReadException(describe(ref, "opening") + " gave no content length", null);
        }
        return new TooLarge(storedLength);
      }
      try {
        Path file = copy(body, ref, storedLength);
        return new Downloaded(file, storedLength, response.eTag(), response.versionId());
      } catch (BodyFailure e) {
        failures.add(e.getCause());
        if (Thread.currentThread().isInterrupted()) {
          throw failure(describe(ref, "downloading") + " was interrupted", failures);
        }
        log.warn(
            "{} broke on attempt {} of {}: {}",
            describe(ref, "downloading"),
            attempt,
            MAX_ATTEMPTS,
            e.getCause().toString());
      }
    }
    throw failure(
        describe(ref, "downloading") + " failed after " + MAX_ATTEMPTS + " attempts", failures);
  }

  /** The last attempt's error is the cause; the earlier attempts' errors are suppressed. */
  private static SourceReadException failure(String message, List<Exception> failures) {
    SourceReadException e = new SourceReadException(message, failures.getLast());
    failures.subList(0, failures.size() - 1).forEach(e::addSuppressed);
    return e;
  }

  /**
   * Copies the body to a new file. A failed read is a {@link BodyFailure} (retried); a failed write
   * is a {@link SourceReadException} (not retried). The file is deleted on any failure.
   */
  private Path copy(ResponseInputStream<GetObjectResponse> body, S3ObjectRef ref, long storedLength)
      throws BodyFailure, SourceReadException {
    Path file = null;
    boolean done = false;
    try {
      try {
        file = Files.createTempFile(spoolDir, "object-", ".part");
      } catch (IOException e) {
        throw new SourceReadException(describe(ref, "creating the local copy of") + " failed", e);
      }
      long copied = 0;
      byte[] buffer = new byte[BUFFER_SIZE];
      try (OutputStream out = outputOpener.open(file)) {
        while (true) {
          int n;
          try {
            n = body.read(buffer);
          } catch (IOException | SdkClientException | UncheckedIOException e) {
            throw new BodyFailure(e);
          }
          if (n < 0) {
            break;
          }
          try {
            out.write(buffer, 0, n);
          } catch (IOException e) {
            throw new SourceReadException(
                describe(ref, "writing the local copy of") + " failed", e);
          }
          copied += n;
        }
      } catch (BodyFailure | SourceReadException e) {
        throw e;
      } catch (IOException e) {
        throw new SourceReadException(describe(ref, "writing the local copy of") + " failed", e);
      }
      if (copied != storedLength) {
        throw new BodyFailure(
            new EOFException(
                "the body ended after %d of %d bytes".formatted(copied, storedLength)));
      }
      done = true;
      return file;
    } finally {
      if (!done) {
        if (file != null) {
          deleteQuietly(file);
        }
        abortQuietly(body);
      }
      closeQuietly(body);
    }
  }

  private static String describe(S3ObjectRef ref, String action) {
    return "%s s3://%s/%s".formatted(action, ref.bucket(), ref.key());
  }

  private static void abortQuietly(ResponseInputStream<GetObjectResponse> body) {
    try {
      body.abort();
    } catch (RuntimeException e) {
      log.debug("aborting the S3 response failed", e);
    }
  }

  private static void closeQuietly(ResponseInputStream<GetObjectResponse> body) {
    try {
      body.close();
    } catch (IOException | RuntimeException e) {
      log.debug("closing the S3 response failed", e);
    }
  }

  private static void deleteQuietly(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      log.warn("failed to delete {}", file, e);
    }
  }

  /** A read of the body failed; the cause is the error to report. */
  private static final class BodyFailure extends Exception {
    BodyFailure(Exception cause) {
      super(cause);
    }

    @Override
    public synchronized Exception getCause() {
      return (Exception) super.getCause();
    }
  }
}
