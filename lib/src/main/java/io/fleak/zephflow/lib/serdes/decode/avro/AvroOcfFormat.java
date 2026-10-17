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
package io.fleak.zephflow.lib.serdes.decode.avro;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

/**
 * Avro object container files, with the container parsed here rather than by DataFileStream, which
 * allocates the size a block header claims and treats a cut-off file as a clean end. A block's size
 * is checked against {@code maxDocumentBytes} before it is read, and its decompressed size while it
 * is decompressed. Blocks are framed, so an oversized block, a damaged compressed block, or a
 * record that does not decode loses only the records of its block; the file must end exactly after
 * a block, and a wrong sync marker ends decoding.
 */
public final class AvroOcfFormat implements FormatCursor {

  private static final byte[] MAGIC = {'O', 'b', 'j', 1};
  private static final Set<String> CODECS =
      Set.of("null", "deflate", "snappy", "zstandard", "bzip2", "xz");

  /** Headers hold the schema and are small; the limit only stops a header that never ends. */
  private static final long MIN_HEADER_LIMIT = 16L * 1024 * 1024;

  private static final byte[] EMPTY = new byte[0];

  private final InputStream in;
  private final long maxDocumentBytes;
  private final DecodeFlags flags;
  private boolean started;
  private String codec;
  private byte[] sync;
  private BoundedDatumReader reader;
  private BoundedDecoder block;
  private long recordsLeft;
  private long blocks;
  private long blockFirstIndex;
  private long index;

  public AvroOcfFormat(InputStream in, DecodeLimits limits, DecodeFlags flags) {
    this.in = new BufferedInputStream(in, 64 * 1024);
    this.maxDocumentBytes = limits.maxDocumentBytes();
    this.flags = flags;
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (!started) {
      started = true;
      if (!readHeader()) {
        return null;
      }
    }
    while (true) {
      if (recordsLeft > 0) {
        return nextRecord();
      }
      if (block != null && !block.isEnd()) {
        int left = block.remaining();
        block = null;
        return new Failed(
            ErrorKind.MALFORMED,
            EMPTY,
            blockFirstIndex,
            null,
            new MalformedAvroException(
                "avro block " + blocks + " has " + left + " bytes left after its records"),
            false);
      }
      block = null;
      DecodeOutcome problem = nextBlock();
      if (problem != null) {
        return problem;
      }
      if (block == null) {
        return null;
      }
    }
  }

  /**
   * A record that does not decode leaves the block position unknown, so the rest of the block is
   * skipped; one that decodes but does not convert loses only itself.
   */
  private DecodeOutcome nextRecord() {
    long recordIndex = ++index;
    recordsLeft--;
    IndexedRecord record;
    try {
      record = reader.read(block);
    } catch (IOException | RuntimeException | StackOverflowError e) {
      index += recordsLeft;
      recordsLeft = 0;
      block = null;
      return new Failed(ErrorKind.MALFORMED, EMPTY, recordIndex, null, failure(e), false);
    }
    try {
      return new Decoded(AvroRecords.toRecord(record), recordIndex, null);
    } catch (RuntimeException | StackOverflowError e) {
      return new Failed(ErrorKind.MALFORMED, EMPTY, recordIndex, null, failure(e), false);
    }
  }

  static Exception failure(Throwable e) {
    return e instanceof Exception exception
        ? exception
        : new MalformedAvroException("avro record nests too deeply for the thread stack", e);
  }

  /** Reads the next block; null with no block at a clean end, or an outcome for a skipped block. */
  private DecodeOutcome nextBlock() throws IOException {
    int first = in.read();
    if (first < 0) {
      return null;
    }
    long count = readLong(first);
    long size = readLong(in.read());
    if (count < 0 || size < 0) {
      throw new MalformedAvroException(
          "avro block " + (blocks + 1) + " has a negative count or size");
    }
    blocks++;
    long firstIndex = index + 1;
    blockFirstIndex = firstIndex;
    if (count > size + BoundedDecoder.ZERO_BYTE_ITEMS) {
      skip(size);
      checkSync();
      index++;
      return new Failed(
          ErrorKind.MALFORMED,
          EMPTY,
          firstIndex,
          null,
          new MalformedAvroException(
              "avro block " + blocks + " claims " + count + " records in " + size + " bytes"),
          false);
    }
    if ((maxDocumentBytes > 0 && size > maxDocumentBytes) || size > AvroBlocks.MAX_BLOCK) {
      skip(size);
      checkSync();
      index += count;
      return new Oversized(firstIndex, null, size, EMPTY, false);
    }
    byte[] compressed = readFully((int) size);
    checkSync();
    byte[] data;
    try {
      data = AvroBlocks.decompress(codec, compressed, maxDocumentBytes);
    } catch (AvroBlocks.TooLarge e) {
      index += count;
      return new Oversized(firstIndex, null, e.size, EMPTY, false);
    } catch (AvroBlocks.CorruptBlockException e) {
      index += count;
      return new Failed(ErrorKind.CORRUPT_COMPRESSION, EMPTY, firstIndex, null, e, false);
    }
    block = new BoundedDecoder(data, 0, data.length);
    try {
      block.takeItems(count, "avro block " + blocks);
    } catch (MalformedAvroException e) {
      block = null;
      index++;
      return new Failed(ErrorKind.MALFORMED, EMPTY, firstIndex, null, e, false);
    }
    recordsLeft = count;
    return null;
  }

  /** False for an empty input. */
  private boolean readHeader() throws IOException {
    byte[] magic = in.readNBytes(MAGIC.length);
    if (magic.length == 0) {
      return false;
    }
    if (!Arrays.equals(magic, MAGIC)) {
      throw new MalformedAvroException("input is not an avro object container file");
    }
    long limit = Math.max(maxDocumentBytes, MIN_HEADER_LIMIT);
    Map<String, byte[]> metadata = new HashMap<>();
    long headerBytes = 0;
    long count;
    while ((count = readLong(in.read())) != 0) {
      if (count < 0) {
        count = -count;
        readLong(in.read());
      }
      for (long i = 0; i < count; i++) {
        byte[] key = readLengthPrefixed(limit - headerBytes);
        byte[] value = readLengthPrefixed(limit - headerBytes - key.length);
        headerBytes += key.length + value.length;
        metadata.put(new String(key, StandardCharsets.UTF_8), value);
      }
    }
    sync = readFully(16);
    byte[] schemaJson = metadata.get("avro.schema");
    if (schemaJson == null) {
      throw new MalformedAvroException("avro file header has no schema");
    }
    Schema schema;
    try {
      schema = new Schema.Parser().parse(new String(schemaJson, StandardCharsets.UTF_8));
    } catch (RuntimeException e) {
      throw new MalformedAvroException("avro file schema is invalid: " + e.getMessage(), e);
    }
    if (schema.getType() != Schema.Type.RECORD) {
      throw new MalformedAvroException(
          "avro file schema is a " + schema.getType() + ", not a record");
    }
    byte[] codecName = metadata.get("avro.codec");
    codec = codecName == null ? "null" : new String(codecName, StandardCharsets.UTF_8);
    if (!CODECS.contains(codec)) {
      throw new MalformedAvroException("avro codec " + codec + " is not supported");
    }
    reader = new BoundedDatumReader(schema);
    return true;
  }

  private byte[] readLengthPrefixed(long limit) throws IOException {
    long length = readLong(in.read());
    if (length < 0 || length > limit) {
      throw new MalformedAvroException(
          "avro file header entry of " + length + " bytes is too long");
    }
    return readFully((int) length);
  }

  private void checkSync() throws IOException {
    if (!Arrays.equals(readFully(16), sync)) {
      throw new MalformedAvroException(
          "avro block " + blocks + " is not followed by the sync marker");
    }
  }

  /** A zig-zag varint whose first byte is already read; -1 there means the input ended. */
  private long readLong(int first) throws IOException {
    long raw = 0;
    int b = first;
    for (int shift = 0; shift < 64; shift += 7) {
      if (b < 0) {
        throw new MalformedAvroException("avro file is truncated");
      }
      raw |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return (raw >>> 1) ^ -(raw & 1);
      }
      b = in.read();
    }
    throw new MalformedAvroException("avro varint is longer than 10 bytes");
  }

  private byte[] readFully(int length) throws IOException {
    byte[] bytes = in.readNBytes(length);
    if (bytes.length < length) {
      throw new MalformedAvroException("avro file is truncated");
    }
    return bytes;
  }

  /** Skips by reading, because some streams' skip() goes past the end of the input. */
  private void skip(long length) throws IOException {
    byte[] scratch = new byte[(int) Math.min(length, 64 * 1024)];
    long left = length;
    while (left > 0) {
      int n = in.read(scratch, 0, (int) Math.min(left, scratch.length));
      if (n < 0) {
        throw new MalformedAvroException("avro file is truncated");
      }
      left -= n;
    }
  }

  @Override
  public long pendingRecordIndex() {
    return index + 1;
  }

  @Override
  public long pendingOffset() {
    return -1;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
