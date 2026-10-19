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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Downloaded;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Missing;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.TooLarge;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3ObjectDownloaderTest {

  private static final S3ObjectRef REF = new S3ObjectRef("b", "k", "us-east-1");
  private static final byte[] DATA = "line one\nline two\n".getBytes(StandardCharsets.UTF_8);
  private static final String ETAG = "\"etag\"";
  private static final String VERSION = "v1";

  /** A thrown exception as a comparable value: its message, its cause and the suppressed causes. */
  record Thrown(String message, String cause, List<String> suppressed) {
    static Thrown of(Throwable e) {
      return new Thrown(
          e.getMessage(),
          e.getCause() == null ? null : e.getCause().toString(),
          Arrays.stream(e.getSuppressed()).map(Throwable::toString).toList());
    }
  }

  @TempDir Path spoolDir;
  private S3Client s3;
  private S3ObjectDownloader downloader;

  @BeforeEach
  void setUp() {
    s3 = mock(S3Client.class);
    downloader = new S3ObjectDownloader(spoolDir);
  }

  @Test
  void downloadsTheWholeObject() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA));

    try (Downloaded downloaded = (Downloaded) downloader.download(s3, REF, 1024)) {
      assertEquals(new Downloaded(downloaded.file(), DATA.length, ETAG, VERSION), downloaded);
      assertArrayEquals(DATA, Files.readAllBytes(downloaded.file()));
      assertEquals(List.of(downloaded.file()), spoolFiles());
    }
    assertEquals(List.of(), spoolFiles());
    verify(s3).getObject(GetObjectRequest.builder().bucket("b").key("k").build());
  }

  static Stream<Arguments> bodyFailures() {
    Supplier<Exception> reset = () -> new IOException("connection reset");
    Supplier<Exception> checksum = () -> RetryableException.create("checksum mismatch");
    Supplier<Exception> unchecked =
        () -> new UncheckedIOException(new IOException("stream closed"));
    return Stream.of(
        Arguments.of(0, reset),
        Arguments.of(1, reset),
        Arguments.of(9, reset),
        Arguments.of(17, reset),
        Arguments.of(DATA.length, checksum),
        Arguments.of(5, unchecked));
  }

  @ParameterizedTest
  @MethodSource("bodyFailures")
  void aBodyThatBreaksIsDownloadedAgainAfterItsPartialFileIsDeleted(
      int breakAt, Supplier<Exception> failure) throws IOException {
    Abortable broken = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, breaking(breakAt, failure), broken))
        .thenAnswer(
            invocation -> {
              assertEquals(List.of(), spoolFiles());
              return body(DATA.length, DATA);
            });

    try (Downloaded downloaded = (Downloaded) downloader.download(s3, REF, 1024)) {
      assertArrayEquals(DATA, Files.readAllBytes(downloaded.file()));
      assertEquals(List.of(downloaded.file()), spoolFiles());
    }
    verify(broken).abort();
    verify(s3, times(2)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void aBodyThatEndsEarlyIsDownloadedAgain() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, new byte[] {'l', 'i'}))
        .thenReturn(body(DATA.length, DATA));

    try (Downloaded downloaded = (Downloaded) downloader.download(s3, REF, 1024)) {
      assertArrayEquals(DATA, Files.readAllBytes(downloaded.file()));
    }
    verify(s3, times(2)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void threeBrokenAttemptsAreAReadFailureCarryingEveryAttemptsError() {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, new byte[] {'l', 'i'}))
        .thenAnswer(
            invocation -> body(DATA.length, breaking(5, resetError()), mock(Abortable.class)));

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals(
        new Thrown(
            "downloading s3://b/k failed after 3 attempts",
            "java.io.IOException: connection reset",
            List.of(
                new EOFException("the body ended after 2 of 18 bytes").toString(),
                "java.io.IOException: connection reset")),
        Thrown.of(e));
    assertEquals(List.of(), spoolFiles());
    verify(s3, times(S3ObjectDownloader.MAX_ATTEMPTS)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void threeShortBodiesReportThePrematureEnd() {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> body(DATA.length, new byte[] {'l'}));

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    String eof = new EOFException("the body ended after 1 of 18 bytes").toString();
    assertEquals(
        new Thrown("downloading s3://b/k failed after 3 attempts", eof, List.of(eof, eof)),
        Thrown.of(e));
  }

  @Test
  void anInterruptedDownloadIsNotRetried() {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            body(
                DATA.length,
                breaking(
                    5,
                    () -> {
                      Thread.currentThread().interrupt();
                      return new IOException("interrupted");
                    }),
                mock(Abortable.class)));

    try {
      SourceReadException e =
          assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));
      assertEquals(
          new Thrown(
              "downloading s3://b/k was interrupted",
              "java.io.IOException: interrupted",
              List.of()),
          Thrown.of(e));
    } finally {
      Thread.interrupted();
    }
    assertEquals(List.of(), spoolFiles());
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void anObjectDeletedBeforeTheFirstAttemptIsMissing() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenThrow(NoSuchKeyException.builder().message("gone").build());

    assertEquals(new Missing(), downloader.download(s3, REF, 1024));
  }

  @Test
  void anObjectDeletedBeforeALaterAttemptIsMissing() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, breaking(5, resetError()), mock(Abortable.class)))
        .thenThrow(NoSuchKeyException.builder().message("gone").build());

    assertEquals(new Missing(), downloader.download(s3, REF, 1024));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void anOpeningErrorIsAReadFailureAndNotRetried() {
    S3Exception denied =
        (S3Exception) S3Exception.builder().statusCode(403).message("Access Denied").build();
    when(s3.getObject(any(GetObjectRequest.class))).thenThrow(denied);

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals(new Thrown("opening s3://b/k failed", denied.toString(), List.of()), Thrown.of(e));
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void aResponseWithoutContentLengthIsAReadFailureAndNotDownloaded() {
    Abortable abortable = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder().eTag(ETAG).build(),
                AbortableInputStream.create(unreadable(), abortable)));

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals(
        new Thrown("opening s3://b/k gave no content length", null, List.of()), Thrown.of(e));
    verify(abortable).abort();
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aFailedWriteIsAReadFailureAndNotRetried() {
    Abortable abortable = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA, abortable));
    IOException diskFull = new IOException("No space left on device");
    S3ObjectDownloader full =
        new S3ObjectDownloader(
            spoolDir,
            file ->
                new FilterOutputStream(Files.newOutputStream(file)) {
                  @Override
                  public void write(byte[] b, int off, int len) throws IOException {
                    throw diskFull;
                  }
                });

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> full.download(s3, REF, 1024));

    assertEquals(
        new Thrown("writing the local copy of s3://b/k failed", diskFull.toString(), List.of()),
        Thrown.of(e));
    verify(abortable).abort();
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aLocalCopyThatCannotBeDeletedDoesNotHideTheReadFailure() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA));
    IOException diskFull = new IOException("No space left on device");
    List<Path> copies = new ArrayList<>();
    S3ObjectDownloader undeletable =
        new S3ObjectDownloader(
            spoolDir,
            file -> {
              // A non-empty directory where the file goes: writing fails, and so does deleting it.
              copies.add(file);
              Files.delete(file);
              Files.createDirectories(file.resolve("child"));
              return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                  throw diskFull;
                }
              };
            });

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> undeletable.download(s3, REF, 1024));

    assertEquals(
        new Thrown("writing the local copy of s3://b/k failed", diskFull.toString(), List.of()),
        Thrown.of(e));
    assertTrue(Files.isDirectory(copies.getFirst()));
  }

  @Test
  void aFailedCloseOfTheLocalCopyIsAReadFailureAndNotRetried() {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA));
    IOException closeFailed = new IOException("close failed");
    S3ObjectDownloader failingClose =
        new S3ObjectDownloader(
            spoolDir,
            file ->
                new FilterOutputStream(Files.newOutputStream(file)) {
                  @Override
                  public void close() throws IOException {
                    super.close();
                    throw closeFailed;
                  }
                });

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> failingClose.download(s3, REF, 1024));

    assertEquals(
        new Thrown("writing the local copy of s3://b/k failed", closeFailed.toString(), List.of()),
        Thrown.of(e));
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aSpoolDirectoryThatIsGoneIsAReadFailureAndNotRetried() throws IOException {
    Abortable abortable = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA, abortable));
    Files.delete(spoolDir);

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    NoSuchFileException missing = assertInstanceOf(NoSuchFileException.class, e.getCause());
    assertEquals(spoolDir, Path.of(missing.getFile()).getParent());
    assertEquals(
        new Thrown("creating the local copy of s3://b/k failed", missing.toString(), List.of()),
        Thrown.of(e));
    verify(abortable).abort();
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void aFailingAbortStillDeletesThePartialFile() {
    Abortable throwing = mock(Abortable.class);
    doThrow(new IllegalStateException("abort failed")).when(throwing).abort();
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, breaking(5, resetError()), throwing))
        .thenAnswer(invocation -> body(DATA.length, breaking(5, resetError()), throwing));

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals(
        new Thrown(
            "downloading s3://b/k failed after 3 attempts",
            "java.io.IOException: connection reset",
            List.of(
                "java.io.IOException: connection reset", "java.io.IOException: connection reset")),
        Thrown.of(e));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void anOpeningErrorAfterABrokenAttemptKeepsTheEarlierError() {
    S3Exception slowDown =
        (S3Exception) S3Exception.builder().statusCode(503).message("SlowDown").build();
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, breaking(5, resetError()), mock(Abortable.class)))
        .thenThrow(slowDown);

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals(
        new Thrown(
            "opening s3://b/k failed",
            slowDown.toString(),
            List.of("java.io.IOException: connection reset")),
        Thrown.of(e));
    verify(s3, times(2)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void aCloseFailureAfterABrokenBodyIsKeptWithTheBodyError() {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenAnswer(
            invocation -> body(DATA.length, breaking(5, resetError()), mock(Abortable.class)));
    S3ObjectDownloader failingClose =
        new S3ObjectDownloader(
            spoolDir,
            file ->
                new FilterOutputStream(Files.newOutputStream(file)) {
                  @Override
                  public void close() throws IOException {
                    super.close();
                    throw new IOException("No space left on device");
                  }
                });

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> failingClose.download(s3, REF, 1024));

    assertEquals(
        List.of("java.io.IOException: No space left on device"),
        Arrays.stream(e.getCause().getSuppressed()).map(Throwable::toString).toList());
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aBodyLongerThanItsLengthIsNotWrittenPastIt() {
    byte[] longer = Arrays.copyOf(DATA, DATA.length + 2);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> body(DATA.length, longer));

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    String longerError = "java.io.IOException: the body is longer than its 18 bytes";
    assertEquals(
        new Thrown(
            "downloading s3://b/k failed after 3 attempts",
            longerError,
            List.of(longerError, longerError)),
        Thrown.of(e));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void anObjectOverTheLimitIsAbortedWithoutReadingItsBody() throws IOException {
    Abortable abortable = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                response(100), AbortableInputStream.create(unreadable(), abortable)));

    assertEquals(new TooLarge(100, ETAG, VERSION), downloader.download(s3, REF, 99));
    verify(abortable).abort();
    assertEquals(List.of(), spoolFiles());
  }

  @ParameterizedTest
  @ValueSource(longs = {18, 1024})
  void anObjectAtOrUnderTheLimitIsDownloaded(long maxObjectSizeBytes) throws IOException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA));

    try (Downloaded downloaded = (Downloaded) downloader.download(s3, REF, maxObjectSizeBytes)) {
      assertArrayEquals(DATA, Files.readAllBytes(downloaded.file()));
    }
  }

  @Test
  void aZeroLengthObjectGivesAnEmptyFile() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(0, new byte[0]));

    try (Downloaded downloaded = (Downloaded) downloader.download(s3, REF, 1024)) {
      assertEquals(new Downloaded(downloaded.file(), 0, ETAG, VERSION), downloaded);
      assertEquals(0, Files.size(downloaded.file()));
    }
    assertEquals(List.of(), spoolFiles());
  }

  private List<Path> spoolFiles() {
    try (Stream<Path> files = Files.list(spoolDir)) {
      return files.toList();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static GetObjectResponse response(long contentLength) {
    return GetObjectResponse.builder()
        .contentLength(contentLength)
        .eTag(ETAG)
        .versionId(VERSION)
        .build();
  }

  private static ResponseInputStream<GetObjectResponse> body(long contentLength, byte[] bytes) {
    return body(contentLength, new ByteArrayInputStream(bytes), mock(Abortable.class));
  }

  private static ResponseInputStream<GetObjectResponse> body(
      long contentLength, byte[] bytes, Abortable abortable) {
    return body(contentLength, new ByteArrayInputStream(bytes), abortable);
  }

  private static ResponseInputStream<GetObjectResponse> body(
      long contentLength, InputStream in, Abortable abortable) {
    return new ResponseInputStream<>(
        response(contentLength), AbortableInputStream.create(in, abortable));
  }

  private static Supplier<Exception> resetError() {
    return () -> new IOException("connection reset");
  }

  private static InputStream unreadable() {
    return new InputStream() {
      @Override
      public int read() {
        throw new AssertionError("the body must not be read");
      }
    };
  }

  /**
   * Delivers the first {@code after} bytes of {@link #DATA}, then throws {@code failure}: a checked
   * {@link IOException} as a reset connection does, or an unchecked one as the SDK does.
   */
  private static InputStream breaking(int after, Supplier<Exception> failure) {
    return new InputStream() {
      private int position;

      @Override
      public int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (position >= after) {
          Exception e = failure.get();
          if (e instanceof IOException io) {
            throw io;
          }
          throw (RuntimeException) e;
        }
        int n = Math.min(len, after - position);
        System.arraycopy(DATA, position, b, off, n);
        position += n;
        return n;
      }
    };
  }
}
