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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.AtLeast;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Copied;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Download;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Ended;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.Opening;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.OverCap;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;

/**
 * The at-least-the-listed-size mode with a cap; the exact mode is tested through the S3 realtime
 * source's downloader.
 */
class SpoolDownloaderTest {

  private static final String TARGET = "gs://b/k";
  private static final byte[] DATA = "line one\nline two\n".getBytes(StandardCharsets.UTF_8);

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
  private SpoolDownloader downloader;
  private final Deque<Opening<String>> openings = new ArrayDeque<>();
  private int opened;

  @BeforeEach
  void setUp() {
    downloader = new SpoolDownloader(spoolDir, "unit-test-", Files::newOutputStream);
  }

  private Download<String> download() throws SourceReadException {
    return downloader.download(
        TARGET,
        () -> {
          opened++;
          return openings.removeFirst();
        });
  }

  @Test
  void aBodyLongerThanListedIsKeptUpToTheCap() throws IOException {
    openings.add(new AtLeast<>(new ByteArrayInputStream(DATA), 5, DATA.length, "info"));

    try (Copied<String> copied = (Copied<String>) download()) {
      assertEquals(new Copied<>(copied.file(), DATA.length, "info"), copied);
      assertArrayEquals(DATA, Files.readAllBytes(copied.file()));
    }
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aFileListedAsEmptyIsCopiedWhole() throws IOException {
    openings.add(new AtLeast<>(new ByteArrayInputStream(DATA), 0, 1024, "info"));

    try (Copied<String> copied = (Copied<String>) download()) {
      assertArrayEquals(DATA, Files.readAllBytes(copied.file()));
    }
  }

  @Test
  void aBodyThatGrowsPastTheCapIsNotKeptOrRetried() throws SourceReadException {
    Abortable abortable = mock(Abortable.class);
    openings.add(
        new AtLeast<>(
            AbortableInputStream.create(new ByteArrayInputStream(DATA), abortable),
            5,
            DATA.length - 1,
            "info"));

    assertEquals(new OverCap<>("info"), download());
    verify(abortable).abort();
    assertEquals(1, opened);
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aFileListedOverTheCapIsNotCopied() throws SourceReadException {
    Abortable abortable = mock(Abortable.class);
    openings.add(
        new AtLeast<>(
            AbortableInputStream.create(unreadable(), abortable), DATA.length, 4, "info"));

    assertEquals(new OverCap<>("info"), download());
    verify(abortable).abort();
    assertEquals(1, opened);
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void aCopyIsNamedByThePrefix() throws IOException {
    openings.add(new AtLeast<>(new ByteArrayInputStream(DATA), DATA.length, 1024, "info"));

    try (Copied<String> copied = (Copied<String>) download()) {
      assertTrue(copied.file().getFileName().toString().startsWith("unit-test-"));
    }
  }

  @Test
  void aBodyShorterThanListedIsDownloadedAgain() throws IOException {
    openings.add(new AtLeast<>(new ByteArrayInputStream(DATA, 0, 4), DATA.length, 1024, "info"));
    openings.add(new AtLeast<>(new ByteArrayInputStream(DATA), DATA.length, 1024, "info"));

    try (Copied<String> copied = (Copied<String>) download()) {
      assertArrayEquals(DATA, Files.readAllBytes(copied.file()));
    }
    assertEquals(2, opened);
  }

  @Test
  void aBodyThatKeepsBreakingFailsAfterTheLastAttempt() {
    Stream.generate(() -> new AtLeast<>(breaking(), DATA.length, 1024, "info"))
        .limit(SpoolDownloader.MAX_ATTEMPTS)
        .forEach(openings::add);

    SourceReadException e = assertThrows(SourceReadException.class, this::download);

    String reset = "java.io.IOException: connection reset";
    assertEquals(
        new Thrown("downloading gs://b/k failed after 3 attempts", reset, List.of(reset, reset)),
        Thrown.of(e));
    assertEquals(List.of(), spoolFiles());
  }

  @Test
  void anEndedOpeningIsTheOutcome() throws SourceReadException {
    openings.add(new Ended<>("missing"));

    assertEquals(new Ended<>("missing"), download());
  }

  @Test
  void anOpeningFailureAfterABrokenAttemptIsNotRetriedAndKeepsTheEarlierError() {
    openings.add(new AtLeast<>(breaking(), DATA.length, 1024, "info"));
    IllegalStateException forbidden = new IllegalStateException("403 Forbidden");

    SourceReadException e =
        assertThrows(
            SourceReadException.class,
            () ->
                downloader.download(
                    TARGET,
                    () -> {
                      if (openings.isEmpty()) {
                        throw forbidden;
                      }
                      return openings.removeFirst();
                    }));

    assertEquals(
        new Thrown(
            "opening gs://b/k failed",
            forbidden.toString(),
            List.of("java.io.IOException: connection reset")),
        Thrown.of(e));
  }

  private List<Path> spoolFiles() {
    try (Stream<Path> files = Files.list(spoolDir)) {
      return files.toList();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static InputStream unreadable() {
    return new InputStream() {
      @Override
      public int read() {
        throw new AssertionError("the body must not be read");
      }
    };
  }

  /** Delivers a few bytes, then fails as a GCS read channel does. */
  private static InputStream breaking() {
    return new InputStream() {
      private boolean delivered;

      @Override
      public int read() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (delivered) {
          throw new IOException("connection reset");
        }
        delivered = true;
        System.arraycopy(DATA, 0, b, off, 4);
        return 4;
      }
    };
  }
}
