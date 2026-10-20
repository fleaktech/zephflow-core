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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdException;
import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Zstandard with our own loop over the streaming context. zstd-jni's input stream reports a clean
 * end when the last of several frames is cut short; here a frame counts as complete only when zstd
 * says so, and input that ends inside a frame is truncated. Trailing bytes that are not a frame
 * fail in zstd itself. Output is handed on in small pieces, so a failure loses at most one piece.
 */
final class ZstdFrameStream extends InputStream {

  private static final int INPUT_CHUNK = 64 * 1024;

  private final InputStream in;
  private final DecodeFlags flags;
  private final ZstdDecompressCtx ctx = new ZstdDecompressCtx();
  private final ByteBuffer src = ByteBuffer.allocateDirect(INPUT_CHUNK).flip();
  private final ByteBuffer dst = ByteBuffer.allocateDirect(LibraryCodecStream.MAX_READ);
  private final byte[] heap = new byte[INPUT_CHUNK];
  private boolean eof;
  private boolean inFrame;
  private int frames;
  private IOException failure;

  ZstdFrameStream(InputStream in, DecodeFlags flags) {
    this.in = in;
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
    while (true) {
      if (!src.hasRemaining()) {
        if (!eof) {
          fill();
          continue;
        }
        if (inFrame) {
          throw fail(corrupt("zstd frame " + (frames + 1) + " is truncated", null));
        }
        return -1;
      }
      dst.clear().limit(Math.min(len, dst.capacity()));
      boolean frameDone;
      try {
        frameDone = ctx.decompressDirectByteBufferStream(dst, src);
      } catch (ZstdException e) {
        throw fail(classify(e));
      }
      inFrame = !frameDone;
      if (frameDone) {
        frames++;
      }
      int n = dst.position();
      if (n > 0) {
        dst.flip().get(b, off, n);
        return n;
      }
    }
  }

  private void fill() throws IOException {
    src.compact();
    int n = in.read(heap, 0, Math.min(heap.length, src.remaining()));
    if (n < 0) {
      eof = true;
    } else {
      src.put(heap, 0, n);
    }
    src.flip();
  }

  /** zstd-jni's exception message is not reliable for these codes, so they are named here. */
  @VisibleForTesting
  IOException classify(ZstdException e) {
    long code = e.getErrorCode();
    if (code == Zstd.errFrameParameterWindowTooLarge()) {
      flags.markBudget(
          new DecodeFlags.BudgetExceeded(
              -1, LimitKind.ZSTD_WINDOW, Decompressors.ZSTD_WINDOW_LIMIT, 0, new byte[0]));
      return new IOException("zstd frame " + (frames + 1) + " needs too much memory", e);
    }
    if (code == Zstd.errMemoryAllocation()) {
      flags.markBudget(
          new DecodeFlags.BudgetExceeded(-1, LimitKind.NATIVE_MEMORY, 0, 0, new byte[0]));
      return new IOException("zstd frame " + (frames + 1) + " could not allocate memory", e);
    }
    String problem;
    if (code == Zstd.errPrefixUnknown()) {
      problem =
          frames == 0
              ? "input is not in zstd format"
              : "unexpected bytes after zstd frame " + frames;
    } else if (code == Zstd.errChecksumWrong()) {
      problem = "zstd frame " + (frames + 1) + " checksum mismatch";
    } else {
      problem = "zstd frame " + (frames + 1) + " is corrupt (error " + code + ")";
    }
    return corrupt(problem, e);
  }

  private CorruptCompressionException corrupt(String message, Throwable cause) {
    CorruptCompressionException e = new CorruptCompressionException(message, cause);
    flags.markDecompress(e);
    return e;
  }

  private IOException fail(IOException e) {
    failure = e;
    return e;
  }

  @Override
  public void close() throws IOException {
    ctx.close();
    in.close();
  }
}
