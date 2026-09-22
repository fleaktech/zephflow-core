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
 */
public final class FsPayloadReader {

  private static final int COPY_BUFFER_BYTES = 8192;
  private static final int GZIP_MAGIC_FIRST_BYTE = 0x1f;
  private static final int GZIP_MAGIC_SECOND_BYTE = 0x8b;
  private static final byte NEWLINE = '\n';

  private final FileReader fileReader;
  private final long maxFileBytes;
  private final int chunkSizeBytes;

  public FsPayloadReader(FileReader fileReader, long maxFileBytes, int chunkSizeBytes) {
    this.fileReader = fileReader;
    this.maxFileBytes = maxFileBytes;
    this.chunkSizeBytes = chunkSizeBytes;
  }

  /** Reads the whole decompressed payload, refusing anything over the cap. */
  public byte[] readWhole(FileKey key) throws IOException {
    try (InputStream inputStream = open(key)) {
      ByteArrayOutputStream payload = new ByteArrayOutputStream();
      byte[] buffer = new byte[COPY_BUFFER_BYTES];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        if ((long) payload.size() + bytesRead > maxFileBytes) {
          throw new PayloadTooLargeException(key.urn(), maxFileBytes);
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
              throw new PayloadTooLargeException(key.urn(), maxFileBytes);
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

  /** The payload exceeded the configured ceiling; the file is skipped, not retried in place. */
  public static class PayloadTooLargeException extends IOException {
    public PayloadTooLargeException(String urn, long maxFileBytes) {
      super("payload for " + urn + " exceeds maxFileBytes=" + maxFileBytes);
    }
  }
}
