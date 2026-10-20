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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.fleak.zephflow.lib.commands.filedecode.LocalSpool;
import io.fleak.zephflow.lib.commands.filedecode.SpoolDownloader.OutputOpener;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.Copied;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.FileBytes;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.InPlace;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.Streamed;
import io.fleak.zephflow.lib.commands.fssource.FsFileAccess.TooLarge;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;

class FsFileAccessTest {

  private static final String URN = "s3://b/k.jsonl";
  private static final byte[] DATA = "line one\nline two\n".getBytes(StandardCharsets.UTF_8);
  private static final long CAP = 1024;

  /** A thrown exception as a comparable value: its type, message and cause. */
  record Thrown(String type, String message, String cause) {
    static Thrown of(Throwable e) {
      return new Thrown(
          e.getClass().getSimpleName(),
          e.getMessage(),
          e.getCause() == null ? null : e.getCause().toString());
    }
  }

  @TempDir Path workingDir;
  private final Deque<Supplier<InputStream>> bodies = new ArrayDeque<>();
  private int opened;
  private int spoolsCreated;
  private Path spoolDir;
  private FileReader reader;

  @BeforeEach
  void setUp() {
    reader =
        (key, offset) -> {
          assertEquals(List.of(URN, 0L), List.of(key.urn(), offset));
          opened++;
          if (spoolDir != null) {
            assertEquals(List.of(), spoolFiles(), "a partial copy is deleted before a retry");
          }
          return bodies.removeFirst().get();
        };
  }

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  private FsFileAccess access(long cap) {
    return access(cap, Files::newOutputStream);
  }

  private FsFileAccess access(long cap, OutputOpener opener) {
    return new FsFileAccess(
        reader,
        false,
        cap,
        () -> {
          spoolsCreated++;
          LocalSpool spool = LocalSpool.create("fs-spool-", workingDir, workingDir);
          spoolDir = spool.getPath();
          return spool;
        },
        opener);
  }

  private static FileEntry entry(long size) {
    return new FileEntry(FileKey.of(URN), size, Instant.EPOCH, "k.jsonl");
  }

  @Test
  void aLocalFileIsReadInPlaceWithoutTheReader() throws Exception {
    Path file = Files.write(workingDir.resolve("data.jsonl"), DATA);
    FsFileAccess access =
        new FsFileAccess(
            (key, offset) -> {
              throw new AssertionError("not read through the reader");
            },
            true,
            CAP);

    FileBytes bytes =
        access.open(
            new FileEntry(FileKey.of(file.toUri().toString()), DATA.length, Instant.EPOCH, "d"),
            false);

    assertArrayEquals(DATA, read(assertInstanceOf(InPlace.class, bytes).input().openStream()));
    assertTrue(assertInstanceOf(InPlace.class, bytes).input().randomAccess().isPresent());
  }

  @Test
  void aRemoteFileIsCopiedIntoTheSpoolAndTheCopyDeletedOnClose() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA));
    FsFileAccess access = access(CAP);
    assertEquals(0, spoolsCreated, "the spool is created on the first copy");

    Copied copied = assertInstanceOf(Copied.class, access.open(entry(DATA.length), false));

    assertEquals(new Copied(copied.input(), copied.file(), DATA.length), copied);
    assertEquals(spoolDir, copied.file().getParent());
    assertTrue(copied.file().getFileName().toString().startsWith("file-"));
    assertArrayEquals(DATA, read(copied.input().openStream()));
    copied.close();
    assertEquals(List.of(), spoolFiles());
    access.close();
    assertFalse(Files.exists(spoolDir));
    assertEquals(1, spoolsCreated);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 9, 17, 18})
  void aBodyThatBreaksIsCopiedAgain(int breakAt) throws Exception {
    bodies.add(() -> breaking(breakAt));
    bodies.add(() -> new ByteArrayInputStream(DATA));

    try (Copied copied = (Copied) access(CAP).open(entry(DATA.length), false)) {
      assertArrayEquals(DATA, Files.readAllBytes(copied.file()));
    }
    assertEquals(2, opened);
  }

  @Test
  void threeBrokenAttemptsAreAReadFailure() {
    Stream.generate(() -> (Supplier<InputStream>) () -> breaking(5)).limit(3).forEach(bodies::add);

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> access(CAP).open(entry(DATA.length), false));

    assertEquals(
        new Thrown(
            "SourceReadException",
            "downloading s3://b/k.jsonl failed after 3 attempts",
            "java.io.IOException: connection reset"),
        Thrown.of(e));
    assertEquals(List.of(3, List.of()), List.of(opened, spoolFiles()));
  }

  @Test
  void aFailingOpenIsNotRetried() {
    RuntimeException denied = new IllegalStateException("AccessDenied");
    reader =
        (key, offset) -> {
          opened++;
          throw denied;
        };

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> access(CAP).open(entry(DATA.length), false));

    assertEquals(
        new Thrown("SourceReadException", "opening s3://b/k.jsonl failed", denied.toString()),
        Thrown.of(e));
    assertEquals(1, opened);
  }

  @Test
  void aFullDiskIsNotRetried() {
    bodies.add(() -> new ByteArrayInputStream(DATA));
    IOException diskFull = new IOException("No space left on device");

    SourceReadException e =
        assertThrows(
            SourceReadException.class,
            () ->
                access(
                        CAP,
                        file ->
                            new FilterOutputStream(Files.newOutputStream(file)) {
                              @Override
                              public void write(byte[] b, int off, int len) throws IOException {
                                throw diskFull;
                              }
                            })
                    .open(entry(DATA.length), false));

    assertEquals(
        new Thrown(
            "SourceReadException",
            "writing the local copy of s3://b/k.jsonl failed",
            diskFull.toString()),
        Thrown.of(e));
    assertEquals(List.of(1, List.of()), List.of(opened, spoolFiles()));
  }

  @Test
  void anInterruptIsNotRetried() {
    bodies.add(
        () ->
            new InputStream() {
              @Override
              public int read() throws IOException {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted");
              }
            });

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> access(CAP).open(entry(DATA.length), false));

    assertEquals(
        new Thrown(
            "SourceReadException",
            "downloading s3://b/k.jsonl was interrupted",
            "java.io.IOException: interrupted"),
        Thrown.of(e));
    assertEquals(List.of(1, List.of()), List.of(opened, spoolFiles()));
  }

  @Test
  void aCopyShorterThanListedIsCopiedAgain() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA, 0, 4));
    bodies.add(() -> new ByteArrayInputStream(DATA));

    try (Copied copied = (Copied) access(CAP).open(entry(DATA.length), false)) {
      assertArrayEquals(DATA, Files.readAllBytes(copied.file()));
    }
    assertEquals(2, opened);
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 5})
  void aCopyLongerThanListedIsKept(long listed) throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA));

    try (Copied copied = (Copied) access(CAP).open(entry(listed), false)) {
      assertEquals(DATA.length, copied.bytes());
      assertArrayEquals(DATA, Files.readAllBytes(copied.file()));
    }
    assertEquals(1, opened);
  }

  @Test
  void aFileThatGrowsPastTheCapIsStreamed() throws Exception {
    Abortable copyAttempt = mock(Abortable.class);
    bodies.add(() -> AbortableInputStream.create(new ByteArrayInputStream(DATA), copyAttempt));
    bodies.add(() -> new ByteArrayInputStream(DATA));

    Streamed streamed =
        assertInstanceOf(Streamed.class, access(DATA.length - 1).open(entry(0), false));

    verify(copyAttempt).abort();
    assertEquals(List.of(), spoolFiles());
    assertArrayEquals(DATA, read(streamed.input().openStream()));
    assertTrue(streamed.input().randomAccess().isEmpty());
    assertEquals(2, opened);
  }

  @Test
  void aFileListedOverTheCapIsStreamedWithoutACopy() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA));

    Streamed streamed = assertInstanceOf(Streamed.class, access(4).open(entry(DATA.length), false));

    assertEquals(List.of(0, 0), List.of(opened, spoolsCreated));
    assertArrayEquals(DATA, read(streamed.input().openStream()));
  }

  @Test
  void aStreamThatEndsBeforeTheListedSizeThrows() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA, 0, 4));
    Streamed streamed = (Streamed) access(4).open(entry(DATA.length), false);

    EOFException e = assertThrows(EOFException.class, () -> read(streamed.input().openStream()));

    assertEquals("the stream of s3://b/k.jsonl ended after 4 of 18 bytes", e.getMessage());
  }

  @Test
  void aShortStreamThrowsWhetherReadByteByByteOrFromAnAbortableBody() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA, 0, 4));
    bodies.add(
        () ->
            AbortableInputStream.create(
                new ByteArrayInputStream(DATA, 0, 4), mock(Abortable.class)));
    Streamed streamed = (Streamed) access(4).open(entry(DATA.length), false);

    List<String> messages = new ArrayList<>();
    try (InputStream in = streamed.input().openStream()) {
      messages.add(
          assertThrows(
                  EOFException.class,
                  () -> {
                    while (in.read() >= 0) {}
                  })
              .getMessage());
    }
    messages.add(
        assertThrows(EOFException.class, () -> read(streamed.input().openStream())).getMessage());

    assertEquals(
        List.of(
            "the stream of s3://b/k.jsonl ended after 4 of 18 bytes",
            "the stream of s3://b/k.jsonl ended after 4 of 18 bytes"),
        messages);
  }

  @Test
  void skippedBytesCountTowardTheListedSize() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA));
    Streamed streamed = (Streamed) access(4).open(entry(DATA.length), false);

    try (InputStream in = streamed.input().openStream()) {
      assertEquals(10, in.skip(10));
      assertArrayEquals(Arrays.copyOfRange(DATA, 10, DATA.length), in.readAllBytes());
    }
  }

  @Test
  void aSpoolThatCannotBeCreatedIsAReadFailure() {
    bodies.add(() -> new ByteArrayInputStream(DATA));
    IOException readOnly = new IOException("Read-only file system");
    FsFileAccess access =
        new FsFileAccess(
            reader,
            false,
            CAP,
            () -> {
              throw new UncheckedIOException(readOnly);
            },
            Files::newOutputStream);

    SourceReadException e =
        assertThrows(SourceReadException.class, () -> access.open(entry(DATA.length), false));

    assertEquals(
        new Thrown(
            "SourceReadException", "creating the spool directory failed", readOnly.toString()),
        Thrown.of(e));
    assertEquals(0, opened);
  }

  @Test
  void aStreamClosedBeforeItsEndIsAbortedAndOneReadToItsEndIsNot() throws Exception {
    Abortable early = mock(Abortable.class);
    Abortable full = mock(Abortable.class);
    bodies.add(() -> AbortableInputStream.create(new ByteArrayInputStream(DATA), early));
    bodies.add(() -> AbortableInputStream.create(new ByteArrayInputStream(DATA), full));
    Streamed streamed = (Streamed) access(4).open(entry(DATA.length), false);

    try (InputStream in = streamed.input().openStream()) {
      assertEquals('l', in.read());
    }
    read(streamed.input().openStream());

    verify(early).abort();
    verify(full, never()).abort();
  }

  @Test
  void aStreamThatCannotBeOpenedThrowsWhenTheDecoderOpensIt() throws Exception {
    reader =
        (key, offset) -> {
          throw new UncheckedIOException(new IOException("failed to open " + key.urn()));
        };
    Streamed streamed = (Streamed) access(4).open(entry(DATA.length), false);

    assertThrows(UncheckedIOException.class, () -> streamed.input().openStream());
  }

  @Test
  void aRandomAccessFileOverTheCapIsTooLarge() throws Exception {
    bodies.add(() -> new ByteArrayInputStream(DATA));

    assertEquals(
        List.of(new TooLarge(), new TooLarge()),
        List.of(
            access(4).open(entry(DATA.length), true),
            access(DATA.length - 1).open(entry(0), true)));
    assertEquals(List.of(), spoolFiles());
  }

  private List<Path> spoolFiles() {
    try (Stream<Path> files = Files.list(spoolDir)) {
      return files.toList();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static byte[] read(InputStream in) throws IOException {
    try (in) {
      return in.readAllBytes();
    }
  }

  /** Delivers the first {@code after} bytes of {@link #DATA}, then fails. */
  private static InputStream breaking(int after) {
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
          throw new IOException("connection reset");
        }
        int n = Math.min(len, after - position);
        System.arraycopy(DATA, position, b, off, n);
        position += n;
        return n;
      }
    };
  }
}
