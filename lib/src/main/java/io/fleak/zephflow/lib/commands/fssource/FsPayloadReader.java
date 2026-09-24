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

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

/**
 * Reads a file's payload without holding the compressed body in memory.
 *
 * <p>Gzip is detected from the first two bytes and decompressed as the stream is consumed, so the
 * cap and the chunk size both count <em>decompressed</em> bytes. That makes the cap a decompression
 * bomb guard as well as a memory guard.
 *
 * <p>{@link #readWhole} is for formats that can only be parsed as one document and is capped at
 * {@code maxFileBytes}. {@link #forEachChunk} is for newline-delimited formats: it has no total
 * size limit, only a per-chunk one. Peak memory is bounded by {@code max(chunkSizeBytes, longest
 * line)}: a single line longer than {@code chunkSizeBytes} must be buffered whole before it can be
 * split, up to {@code maxFileBytes}, at which point it is rejected rather than buffered further.
 * {@link #stream} is for formats a parser reads record by record (json array, csv): it has no total
 * size limit either, and {@code maxFileBytes} caps a single record instead.
 */
public final class FsPayloadReader {

  private static final int COPY_BUFFER_BYTES = 8192;
  private static final int GZIP_MAGIC_FIRST_BYTE = 0x1f;
  private static final int GZIP_MAGIC_SECOND_BYTE = 0x8b;
  private static final byte NEWLINE = '\n';

  /**
   * How far past {@code maxFileBytes} a record may run before {@link #stream} rejects it. Parsers
   * read ahead in blocks of about 8 KiB, so the stream sees bytes of the next record before the
   * current one is marked done; the slack keeps that read-ahead from tripping the cap.
   */
  static final int RECORD_READ_AHEAD_SLACK_BYTES = 64 * 1024;

  private final FileReader fileReader;
  private final long maxFileBytes;
  private final int chunkSizeBytes;

  public FsPayloadReader(FileReader fileReader, long maxFileBytes, int chunkSizeBytes) {
    this.fileReader = fileReader;
    this.maxFileBytes = maxFileBytes;
    this.chunkSizeBytes = chunkSizeBytes;
  }

  /** Target size of one chunk or batch, in decompressed bytes. */
  public int chunkSizeBytes() {
    return chunkSizeBytes;
  }

  /** Reads the whole decompressed payload, refusing anything over the cap. */
  public byte[] readWhole(FileKey key) throws IOException {
    try (InputStream inputStream = open(key)) {
      ByteArrayOutputStream payload = new ByteArrayOutputStream();
      byte[] buffer = new byte[COPY_BUFFER_BYTES];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        if ((long) payload.size() + bytesRead > maxFileBytes) {
          throw PayloadTooLargeException.wholePayload(
              (long) payload.size() + bytesRead, maxFileBytes);
        }
        payload.write(buffer, 0, bytesRead);
      }
      return payload.toByteArray();
    }
  }

  /**
   * Feeds the payload to {@code chunkConsumer} in pieces of at least {@code chunkSizeBytes} that
   * always end on a newline, so each piece is a whole number of records. The final piece may lack a
   * trailing newline.
   */
  public void forEachChunk(FileKey key, Consumer<byte[]> chunkConsumer) throws IOException {
    try (InputStream inputStream = open(key)) {
      ScanningBuffer pending = new ScanningBuffer(chunkSizeBytes);
      byte[] buffer = new byte[COPY_BUFFER_BYTES];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        pending.write(buffer, 0, bytesRead);
        // One read can hold many chunks, so drain the buffer rather than emitting once per read.
        while (pending.size() >= chunkSizeBytes) {
          int splitIndex = pending.indexOfNewlineAtOrAfter(chunkSizeBytes - 1);
          if (splitIndex < 0) {
            // No record boundary at or past the chunk size (yet): this line already spans a whole
            // chunk. The bytes already scanned for a newline are remembered so the next read only
            // scans what it newly contributes, instead of rescanning everything buffered so far.
            if (pending.size() > maxFileBytes) {
              throw PayloadTooLargeException.singleLine(pending.size(), maxFileBytes);
            }
            break;
          }
          chunkConsumer.accept(pending.copyRange(0, splitIndex + 1));
          pending.discardThrough(splitIndex);
        }
      }
      if (pending.size() > 0) {
        chunkConsumer.accept(pending.copyRange(0, pending.size()));
      }
    }
  }

  /** Reads a {@link RecordStream}; may throw what reading it throws. */
  @FunctionalInterface
  public interface StreamConsumer {
    void accept(RecordStream input) throws IOException;
  }

  /**
   * Hands {@code consumer} the decompressed payload as a stream, for a parser that reads it record
   * by record. The consumer calls {@link RecordStream#recordBoundary} after each record; a record
   * that runs past {@code maxFileBytes} without one fails the read with {@link
   * PayloadTooLargeException}, since the parser is buffering it whole.
   */
  public void stream(FileKey key, StreamConsumer consumer) throws IOException {
    try (InputStream inputStream = open(key)) {
      consumer.accept(
          new RecordStream(
              inputStream, maxFileBytes + RECORD_READ_AHEAD_SLACK_BYTES, maxFileBytes));
    }
  }

  /** A stream that counts the bytes read and refuses one record that outgrows the cap. */
  public static final class RecordStream extends FilterInputStream {
    private final long recordLimitBytes;
    private final long maxFileBytes;
    private long bytesRead = 0;
    private long bytesReadAtBoundary = 0;

    private RecordStream(InputStream in, long recordLimitBytes, long maxFileBytes) {
      super(in);
      this.recordLimitBytes = recordLimitBytes;
      this.maxFileBytes = maxFileBytes;
    }

    /** Total decompressed bytes read so far. */
    public long bytesRead() {
      return bytesRead;
    }

    /** Marks the end of a record: the cap applies to the bytes read after this point. */
    public void recordBoundary() {
      bytesReadAtBoundary = bytesRead;
    }

    @Override
    public int read() throws IOException {
      int value = super.read();
      if (value != -1) {
        count(1);
      }
      return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int read = super.read(buffer, offset, length);
      if (read > 0) {
        count(read);
      }
      return read;
    }

    @Override
    public long skip(long n) throws IOException {
      long skipped = super.skip(n);
      count(skipped);
      return skipped;
    }

    private void count(long read) throws PayloadTooLargeException {
      bytesRead += read;
      if (bytesRead - bytesReadAtBoundary > recordLimitBytes) {
        throw PayloadTooLargeException.singleRecord(bytesRead - bytesReadAtBoundary, maxFileBytes);
      }
    }
  }

  /**
   * A growable byte buffer that tracks how much of itself has already been scanned for a newline,
   * so repeated scans while a line is still growing examine only the newly appended bytes rather
   * than rescanning from the start. This is what keeps {@link #forEachChunk} linear in payload
   * size: without it, a long newline-free line forces a full rescan (and, with a plain {@link
   * ByteArrayOutputStream}, a full copy) on every read.
   */
  private static final class ScanningBuffer extends ByteArrayOutputStream {
    private int scannedThrough = 0;

    ScanningBuffer(int initialCapacity) {
      super(initialCapacity);
    }

    /** Index of the first newline at or after {@code from}, or -1. Scans each byte at most once. */
    int indexOfNewlineAtOrAfter(int from) {
      int start = Math.max(from, scannedThrough);
      for (int index = start; index < count; index++) {
        if (buf[index] == NEWLINE) {
          return index;
        }
      }
      scannedThrough = count;
      return -1;
    }

    byte[] copyRange(int from, int to) {
      return Arrays.copyOfRange(buf, from, to);
    }

    /** Drops bytes {@code [0, throughIndex]}, keeping the remainder as the new buffer content. */
    void discardThrough(int throughIndex) {
      int remaining = count - throughIndex - 1;
      System.arraycopy(buf, throughIndex + 1, buf, 0, remaining);
      count = remaining;
      scannedThrough = 0;
    }
  }

  private InputStream open(FileKey key) throws IOException {
    PushbackInputStream pushbackInputStream = new PushbackInputStream(fileReader.open(key, 0), 2);
    byte[] magicBytes = new byte[2];
    int bytesRead;
    try {
      bytesRead = pushbackInputStream.readNBytes(magicBytes, 0, 2);
    } catch (IOException | RuntimeException failure) {
      // Detection reads before the caller's try-with-resources takes ownership of the stream, so a
      // failure here must close it itself or the underlying connection (an S3 pooled HTTP
      // connection, for one) leaks.
      pushbackInputStream.close();
      throw failure;
    }
    if (bytesRead > 0) {
      pushbackInputStream.unread(magicBytes, 0, bytesRead);
    }
    boolean gzipped =
        bytesRead == 2
            && (magicBytes[0] & 0xff) == GZIP_MAGIC_FIRST_BYTE
            && (magicBytes[1] & 0xff) == GZIP_MAGIC_SECOND_BYTE;
    if (!gzipped) {
      return pushbackInputStream;
    }
    try {
      return new GZIPInputStream(pushbackInputStream);
    } catch (IOException | RuntimeException failure) {
      // The GZIPInputStream constructor reads the gzip header eagerly and can throw for a file
      // whose magic bytes are valid but whose body is corrupt. It does not close the stream it was
      // given on failure, so without this the underlying connection (an S3 pooled HTTP connection,
      // for one) leaks, and since a failing file is retried on every run, the leak recurs until the
      // pool is exhausted.
      pushbackInputStream.close();
      throw failure;
    }
  }

  /**
   * The payload exceeded the configured ceiling; the file is skipped, not retried in place.
   *
   * <p>The two factory methods describe genuinely different situations that need different operator
   * responses, so they say so rather than sharing one message. The urn is left out: every caller
   * already logs it alongside.
   */
  public static class PayloadTooLargeException extends IOException {
    private PayloadTooLargeException(String message) {
      super(message);
    }

    /** A whole-document format whose payload passed the cap while being buffered. */
    static PayloadTooLargeException wholePayload(long bytesRead, long maxFileBytes) {
      return new PayloadTooLargeException(
          String.format(
              "file is too large to read as a whole document: exceeded the %,d byte maxFileBytes"
                  + " limit after reading %,d bytes. Raise maxFileBytes, or switch to an encoding"
                  + " that is streamed instead of buffered (JSON_OBJECT_LINE, STRING_LINE, CSV or"
                  + " JSON_ARRAY).",
              maxFileBytes, bytesRead));
    }

    /** A line-delimited format where one line grew past the cap with no newline to split on. */
    static PayloadTooLargeException singleLine(long bytesBuffered, long maxFileBytes) {
      return new PayloadTooLargeException(
          String.format(
              "a single line is too large to split: buffered %,d bytes with no newline, past the"
                  + " %,d byte maxFileBytes limit. A line must fit in memory to be emitted; raise"
                  + " maxFileBytes, or check the file really is newline-delimited.",
              bytesBuffered, maxFileBytes));
    }

    /** A streamed format where one record (an array element, a csv row) grew past the cap. */
    static PayloadTooLargeException singleRecord(long bytesRead, long maxFileBytes) {
      return new PayloadTooLargeException(
          String.format(
              "a single record is too large to read: read %,d bytes without reaching its end, past"
                  + " the %,d byte maxFileBytes limit. A record must fit in memory to be emitted;"
                  + " raise maxFileBytes, or check the file is well formed (an unclosed quote in a"
                  + " csv file makes the rest of the file one field).",
              bytesRead, maxFileBytes));
    }
  }
}
