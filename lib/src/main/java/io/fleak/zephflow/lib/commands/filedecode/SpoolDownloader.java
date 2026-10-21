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
package io.fleak.zephflow.lib.commands.filedecode;

import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.Abortable;

/**
 * Downloads one file whole into a spool directory, so that nothing is decoded while the network is
 * involved. A body that breaks mid-download is downloaded again from the beginning, up to {@link
 * #MAX_ATTEMPTS} attempts; opening errors, local write failures and interrupts are not retried.
 */
@Slf4j
public final class SpoolDownloader {

  public static final int MAX_ATTEMPTS = 3;
  private static final int BUFFER_SIZE = 64 * 1024;

  private final Path spoolDir;
  private final String filePrefix;
  private final OutputOpener outputOpener;

  /**
   * @param filePrefix the start of each local copy's name
   */
  public SpoolDownloader(Path spoolDir, String filePrefix, OutputOpener outputOpener) {
    this.spoolDir = spoolDir;
    this.filePrefix = filePrefix;
    this.outputOpener = outputOpener;
  }

  /** Opens the local copy for writing; replaceable so tests can fail the writes. */
  @FunctionalInterface
  public interface OutputOpener {
    OutputStream open(Path file) throws IOException;
  }

  /**
   * Opens the body for one attempt. A {@link SourceReadException} or a runtime exception ends the
   * download without a retry.
   */
  @FunctionalInterface
  public interface Opener<T> {
    Opening<T> open() throws SourceReadException;
  }

  /** What one attempt opened. */
  public sealed interface Opening<T> permits Exact, AtLeast, Ended {}

  /**
   * A body that must be exactly {@code length} bytes; a longer or shorter one fails the attempt.
   */
  public record Exact<T>(InputStream body, long length, T info) implements Opening<T> {}

  /**
   * A body that must be at least {@code length} bytes; one that is longer than {@code capBytes} is
   * not kept ({@link OverCap}), and one listed longer is not copied at all.
   */
  public record AtLeast<T>(InputStream body, long length, long capBytes, T info)
      implements Opening<T> {}

  /** Nothing to download: the opener decided the outcome and released its body. */
  public record Ended<T>(T info) implements Opening<T>, Download<T> {}

  /** How {@link #download} ended. */
  public sealed interface Download<T> permits Copied, Ended, OverCap {}

  /** The bytes are in {@code file}; closing deletes it. */
  public record Copied<T>(Path file, long bytes, T info) implements Download<T>, AutoCloseable {
    @Override
    public void close() {
      deleteQuietly(file);
    }
  }

  /** An {@link AtLeast} body grew past its cap; its partial copy is deleted. */
  public record OverCap<T>(T info) implements Download<T> {}

  /**
   * @param target the file's location in error messages, such as {@code s3://bucket/key}
   */
  public <T> Download<T> download(String target, Opener<T> opener) throws SourceReadException {
    List<Exception> failures = new ArrayList<>();
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      Opening<T> opening;
      try {
        opening = opener.open();
      } catch (SourceReadException e) {
        throw withEarlierAttempts(e, failures);
      } catch (RuntimeException e) {
        throw withEarlierAttempts(
            new SourceReadException(describe(target, "opening") + " failed", e), failures);
      }
      try {
        return switch (opening) {
          case Ended<T> ended -> ended;
          case Exact<T> exact -> {
            Local local = copy(exact.body(), target, exact.length(), exact.length(), true);
            yield new Copied<>(local.file(), local.bytes(), exact.info());
          }
          case AtLeast<T> atLeast when atLeast.length() > atLeast.capBytes() -> {
            discard(atLeast.body());
            yield new OverCap<>(atLeast.info());
          }
          case AtLeast<T> atLeast -> {
            Local local = copy(atLeast.body(), target, atLeast.length(), atLeast.capBytes(), false);
            yield local == null
                ? new OverCap<>(atLeast.info())
                : new Copied<>(local.file(), local.bytes(), atLeast.info());
          }
        };
      } catch (BodyFailure e) {
        Exception cause = e.getCause();
        Arrays.stream(e.getSuppressed()).forEach(cause::addSuppressed);
        failures.add(cause);
        if (Thread.currentThread().isInterrupted()) {
          throw failure(describe(target, "downloading") + " was interrupted", failures);
        }
        log.warn(
            "{} broke on attempt {} of {}: {}",
            describe(target, "downloading"),
            attempt,
            MAX_ATTEMPTS,
            cause.toString());
      }
    }
    throw failure(
        describe(target, "downloading") + " failed after " + MAX_ATTEMPTS + " attempts", failures);
  }

  /** Aborts the body's connection when it supports that, then closes it, ignoring failures. */
  public static void discard(InputStream body) {
    abortQuietly(body);
    closeQuietly(body);
  }

  /** The last attempt's error is the cause; the earlier attempts' errors are suppressed. */
  private static SourceReadException failure(String message, List<Exception> failures) {
    return withEarlierAttempts(
        new SourceReadException(message, failures.getLast()),
        failures.subList(0, failures.size() - 1));
  }

  private static SourceReadException withEarlierAttempts(
      SourceReadException e, List<Exception> earlier) {
    earlier.forEach(e::addSuppressed);
    return e;
  }

  /**
   * Copies the body to a new file of at least {@code minBytes} and at most {@code maxBytes}. A
   * failed read, a short body, and with {@code overMaxFails} a long one, are a {@link BodyFailure}
   * (retried); a failed write is a {@link SourceReadException} (not retried). Returns null when the
   * body is longer than {@code maxBytes} and {@code overMaxFails} is false. The file is deleted
   * unless it is returned.
   */
  private Local copy(
      InputStream body, String target, long minBytes, long maxBytes, boolean overMaxFails)
      throws BodyFailure, SourceReadException {
    Path file = null;
    boolean done = false;
    try {
      try {
        file = Files.createTempFile(spoolDir, filePrefix, ".part");
      } catch (IOException e) {
        throw new SourceReadException(
            describe(target, "creating the local copy of") + " failed", e);
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
          if (copied + n > maxBytes) {
            if (overMaxFails) {
              throw new BodyFailure(
                  new IOException("the body is longer than its %d bytes".formatted(minBytes)));
            }
            return null;
          }
          try {
            out.write(buffer, 0, n);
          } catch (IOException e) {
            throw new SourceReadException(
                describe(target, "writing the local copy of") + " failed", e);
          }
          copied += n;
        }
      } catch (BodyFailure | SourceReadException e) {
        throw e;
      } catch (IOException e) {
        throw new SourceReadException(describe(target, "writing the local copy of") + " failed", e);
      }
      if (copied < minBytes) {
        throw new BodyFailure(
            new EOFException("the body ended after %d of %d bytes".formatted(copied, minBytes)));
      }
      done = true;
      return new Local(file, copied);
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

  private static String describe(String target, String action) {
    return action + " " + target;
  }

  private static void abortQuietly(InputStream body) {
    if (!(body instanceof Abortable abortable)) {
      return;
    }
    try {
      abortable.abort();
    } catch (RuntimeException e) {
      log.debug("aborting the response failed", e);
    }
  }

  private static void closeQuietly(InputStream body) {
    try {
      body.close();
    } catch (IOException | RuntimeException e) {
      log.debug("closing the response failed", e);
    }
  }

  /** Deletes {@code file} if it exists, logging a failure. */
  public static void deleteQuietly(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (IOException e) {
      log.warn("failed to delete {}", file, e);
    }
  }

  private record Local(Path file, long bytes) {}

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
