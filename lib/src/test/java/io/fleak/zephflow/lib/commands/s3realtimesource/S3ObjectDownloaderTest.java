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

import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Download;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Downloaded;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.Missing;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDownloader.TooLarge;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.ResponseInputStream;
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

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 9, 17})
  void aBodyThatBreaksIsDownloadedAgainAfterItsPartialFileIsDeleted(int breakAt)
      throws IOException {
    Abortable broken = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(body(DATA.length, breaking(breakAt), broken))
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
  void threeBrokenAttemptsAreAReadFailure() {
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenAnswer(invocation -> body(DATA.length, breaking(5), mock(Abortable.class)));

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals("downloading s3://b/k failed after 3 attempts", e.getMessage());
    assertEquals("connection reset", e.getCause().getMessage());
    assertEquals(List.of(), spoolFiles());
    verify(s3, times(S3ObjectDownloader.MAX_ATTEMPTS)).getObject(any(GetObjectRequest.class));
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
        .thenReturn(body(DATA.length, breaking(5), mock(Abortable.class)))
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

    assertEquals("opening s3://b/k failed", e.getMessage());
    assertSame(denied, e.getCause());
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void aFailedLocalWriteIsAReadFailureAndNotRetried() throws IOException {
    Abortable abortable = mock(Abortable.class);
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA, abortable));
    Files.delete(spoolDir);

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> downloader.download(s3, REF, 1024));

    assertEquals("creating the local copy of s3://b/k failed", e.getMessage());
    verify(abortable).abort();
    verify(s3, times(1)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void anObjectOverTheLimitIsAbortedWithoutReadingItsBody() throws IOException {
    Abortable abortable = mock(Abortable.class);
    InputStream unread =
        new InputStream() {
          @Override
          public int read() {
            throw new AssertionError("the body must not be read");
          }
        };
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                response(100), AbortableInputStream.create(unread, abortable)));

    assertEquals(new TooLarge(100), downloader.download(s3, REF, 99));
    verify(abortable).abort();
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void anObjectAtTheLimitIsDownloaded() throws IOException {
    when(s3.getObject(any(GetObjectRequest.class))).thenReturn(body(DATA.length, DATA));

    Download download = downloader.download(s3, REF, DATA.length);

    try (Downloaded downloaded = (Downloaded) download) {
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

  /**
   * Delivers the first {@code after} bytes of {@link #DATA}, then fails like a reset connection.
   */
  private static InputStream breaking(int after) {
    return new InputStream() {
      private int position;

      @Override
      public int read() throws IOException {
        if (position >= after) {
          throw new IOException("connection reset");
        }
        return DATA[position++] & 0xff;
      }
    };
  }
}
