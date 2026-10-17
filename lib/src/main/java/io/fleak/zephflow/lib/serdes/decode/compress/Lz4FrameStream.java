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
package io.fleak.zephflow.lib.serdes.decode.compress;

import io.fleak.zephflow.lib.serdes.decode.internal.ByteBuilder;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.commons.compress.compressors.lz4.BlockLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.XXHash32;

/**
 * The LZ4 frame format with our own frame loop; commons-compress only decodes block contents.
 * commons-compress's frame reader ends cleanly when the input stops right after an uncompressed
 * block's size, so the structure is checked here: a frame ends only at its end mark (and content
 * checksum), anything else at the end of the input is truncation. A block is handed on after its
 * checksum, when the frame has one, has been checked.
 */
final class Lz4FrameStream extends InputStream {

  private static final int MAGIC = 0x184D2204;
  private static final int SKIPPABLE_MAGIC = 0x184D2A50;
  private static final int WINDOW = 64 * 1024;

  private final InputStream in;
  private final DecodeFlags flags;
  private byte[] block = new byte[0];
  private int blockPos;
  private byte[] dictionary = new byte[0];
  private final XXHash32 content = new XXHash32();
  private boolean inFrame;
  private boolean independent;
  private boolean blockChecksum;
  private boolean contentChecksum;
  private long contentSize;
  private int maxBlock;
  private long produced;
  private int frames;
  private IOException failure;

  Lz4FrameStream(InputStream in, DecodeFlags flags) {
    this.in = new BufferedInputStream(in, 64 * 1024);
    this.flags = flags;
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    int n = read(one, 0, 1);
    return n < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (failure != null) {
      throw failure;
    }
    if (len == 0) {
      return 0;
    }
    while (blockPos == block.length) {
      if (!nextBlock()) {
        return -1;
      }
    }
    int n = Math.min(len, block.length - blockPos);
    System.arraycopy(block, blockPos, b, off, n);
    blockPos += n;
    return n;
  }

  /** False at a clean end of the input, between frames. */
  private boolean nextBlock() throws IOException {
    if (!inFrame && !startFrame()) {
      return false;
    }
    int size = readInt("lz4 frame " + (frames + 1) + " is truncated");
    if (size == 0) {
      endFrame();
      setBlock(new byte[0]);
      return true;
    }
    boolean uncompressed = size < 0;
    int length = size & 0x7FFFFFFF;
    if (length > maxBlock) {
      throw corrupt("lz4 block of " + length + " bytes exceeds the frame's block size", null);
    }
    byte[] data = readExactly(length);
    if (blockChecksum && readInt("lz4 frame " + (frames + 1) + " is truncated") != hash(data)) {
      throw corrupt("lz4 block checksum mismatch in frame " + (frames + 1), null);
    }
    byte[] decoded = uncompressed ? data : decompress(data);
    content.update(decoded, 0, decoded.length);
    produced += decoded.length;
    if (!independent) {
      remember(decoded);
    }
    setBlock(decoded);
    return true;
  }

  private byte[] decompress(byte[] data) throws IOException {
    ByteBuilder out = new ByteBuilder();
    try (BlockLZ4CompressorInputStream block =
        new BlockLZ4CompressorInputStream(new ByteArrayInputStream(data))) {
      if (!independent && dictionary.length > 0) {
        block.prefill(dictionary);
      }
      byte[] buf = new byte[8 * 1024];
      int n;
      while ((n = block.read(buf, 0, buf.length)) >= 0) {
        if (out.size() + n > maxBlock) {
          throw corrupt("lz4 block decodes to more than the frame's block size", null);
        }
        out.append(buf, 0, n);
      }
    } catch (CorruptCompressionException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw corrupt("lz4 block is corrupt: " + e.getMessage(), e);
    }
    return out.toByteArray();
  }

  private void remember(byte[] decoded) {
    if (decoded.length >= WINDOW) {
      dictionary = Arrays.copyOfRange(decoded, decoded.length - WINDOW, decoded.length);
      return;
    }
    int keep = Math.min(dictionary.length, WINDOW - decoded.length);
    byte[] next = new byte[keep + decoded.length];
    System.arraycopy(dictionary, dictionary.length - keep, next, 0, keep);
    System.arraycopy(decoded, 0, next, keep, decoded.length);
    dictionary = next;
  }

  private void setBlock(byte[] data) {
    block = data;
    blockPos = 0;
  }

  /** Skips skippable frames; false at a clean end of the input. */
  private boolean startFrame() throws IOException {
    while (true) {
      byte[] head = in.readNBytes(4);
      if (head.length == 0) {
        return false;
      }
      if (head.length < 4) {
        throw corrupt(
            startsLikeAFrame(head) ? "lz4 frame " + (frames + 1) + " is truncated" : notAFrame(),
            null);
      }
      int magic = littleEndian(head, 0);
      if ((magic & 0xFFFFFFF0) == SKIPPABLE_MAGIC) {
        skipFrame(readInt("lz4 skippable frame is truncated") & 0xFFFFFFFFL);
        continue;
      }
      if (magic != MAGIC) {
        throw corrupt(notAFrame(), null);
      }
      readDescriptor();
      return true;
    }
  }

  private void readDescriptor() throws IOException {
    String truncated = "lz4 frame " + (frames + 1) + " is truncated";
    int flg = readByte(truncated);
    int bd = readByte(truncated);
    if ((flg >> 6) != 1 || (flg & 0x02) != 0 || (bd & 0x8F) != 0) {
      throw corrupt("lz4 frame " + (frames + 1) + " has an unsupported header", null);
    }
    if ((flg & 0x01) != 0) {
      throw corrupt("lz4 frames with a dictionary are not supported", null);
    }
    int sizeCode = (bd >> 4) & 0x07;
    if (sizeCode < 4) {
      throw corrupt("lz4 frame " + (frames + 1) + " has an invalid block size", null);
    }
    independent = (flg & 0x20) != 0;
    blockChecksum = (flg & 0x10) != 0;
    contentChecksum = (flg & 0x04) != 0;
    maxBlock = 1 << (8 + 2 * sizeCode);
    ByteBuilder descriptor = new ByteBuilder();
    descriptor.append(flg);
    descriptor.append(bd);
    contentSize = -1;
    if ((flg & 0x08) != 0) {
      byte[] size = readExactly(8);
      descriptor.append(size, 0, 8);
      contentSize = littleEndian(size, 0) & 0xFFFFFFFFL | (long) littleEndian(size, 4) << 32;
    }
    int checksum = readByte(truncated);
    byte[] described = descriptor.toByteArray();
    if (((hash(described) >> 8) & 0xFF) != checksum) {
      throw corrupt("lz4 frame " + (frames + 1) + " header checksum mismatch", null);
    }
    inFrame = true;
    dictionary = new byte[0];
    content.reset();
    produced = 0;
  }

  private void endFrame() throws IOException {
    if (contentChecksum
        && readInt("lz4 frame " + (frames + 1) + " is truncated") != (int) content.getValue()) {
      throw corrupt("lz4 frame " + (frames + 1) + " content checksum mismatch", null);
    }
    if (contentSize >= 0 && produced != contentSize) {
      throw corrupt("lz4 frame " + (frames + 1) + " content size mismatch", null);
    }
    frames++;
    inFrame = false;
  }

  private String notAFrame() {
    return frames == 0
        ? "input is not in lz4 frame format"
        : "unexpected bytes after lz4 frame " + frames;
  }

  private static boolean startsLikeAFrame(byte[] head) {
    for (int i = 0; i < head.length; i++) {
      if ((head[i] & 0xFF) != ((MAGIC >>> (8 * i)) & 0xFF)) {
        return false;
      }
    }
    return true;
  }

  private static int hash(byte[] data) {
    XXHash32 hash = new XXHash32();
    hash.update(data, 0, data.length);
    return (int) hash.getValue();
  }

  private int readInt(String truncated) throws IOException {
    byte[] bytes = in.readNBytes(4);
    if (bytes.length < 4) {
      throw corrupt(truncated, null);
    }
    return littleEndian(bytes, 0);
  }

  private int readByte(String truncated) throws IOException {
    int b = in.read();
    if (b < 0) {
      throw corrupt(truncated, null);
    }
    return b;
  }

  private byte[] readExactly(int length) throws IOException {
    byte[] data = in.readNBytes(length);
    if (data.length < length) {
      throw corrupt("lz4 frame " + (frames + 1) + " is truncated", null);
    }
    return data;
  }

  private void skipFrame(long length) throws IOException {
    long left = length;
    while (left > 0) {
      long skipped = in.skip(left);
      if (skipped <= 0) {
        if (in.read() < 0) {
          throw corrupt("lz4 skippable frame is truncated", null);
        }
        skipped = 1;
      }
      left -= skipped;
    }
  }

  private static int littleEndian(byte[] b, int at) {
    return (b[at] & 0xFF)
        | (b[at + 1] & 0xFF) << 8
        | (b[at + 2] & 0xFF) << 16
        | (b[at + 3] & 0xFF) << 24;
  }

  private CorruptCompressionException corrupt(String message, Throwable cause) {
    CorruptCompressionException e = new CorruptCompressionException(message, cause);
    flags.markDecompress(e);
    failure = e;
    return e;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
