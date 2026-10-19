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

import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Runs a codec library's stream under the decoder's rules. The library is opened on the first read,
 * and an input with no bytes at all decodes to nothing. Reads are kept small because a library that
 * fails drops what it had already decoded in the same call; at most {@link #MAX_READ} - 1 bytes can
 * be lost that way. Library failures are marked as decompression failures, unless a layer
 * underneath already marked its own failure.
 */
final class LibraryCodecStream extends InputStream {

  static final int MAX_READ = 256;

  @FunctionalInterface
  interface Opener {
    InputStream open(InputStream compressed) throws IOException;
  }

  /** Maps a library failure that means "needs more memory than allowed" to its size, or -1. */
  @FunctionalInterface
  interface MemoryNeeded {
    long bytes(Exception e);
  }

  private final String name;
  private final InputStream in;
  private final Opener opener;
  private final MemoryNeeded memoryNeeded;
  private final long memoryLimit;
  private final DecodeFlags flags;
  private InputStream library;
  private boolean empty;
  private IOException failure;

  /** {@code memoryLimit} is the memory the library may use, reported when it needs more. */
  LibraryCodecStream(
      String name,
      InputStream in,
      Opener opener,
      MemoryNeeded memoryNeeded,
      long memoryLimit,
      DecodeFlags flags) {
    this.name = name;
    this.in = in;
    this.opener = opener;
    this.memoryNeeded = memoryNeeded;
    this.memoryLimit = memoryLimit;
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
    try {
      if (library == null && !empty) {
        PeekedStream peeked = PeekedStream.peek(in, 1, head -> head.length > 0);
        if (peeked.head().length == 0) {
          empty = true;
        } else {
          library = opener.open(peeked);
        }
      }
      return empty ? -1 : library.read(b, off, Math.min(len, MAX_READ));
    } catch (IOException | RuntimeException e) {
      failure = classify(e);
      throw failure;
    }
  }

  /** Failures another layer already marked (a read failure, an outer codec's limit) keep it. */
  private IOException classify(Exception e) {
    if (flags.anySet()) {
      return e instanceof IOException io ? io : new IOException(e);
    }
    long needed = memoryNeeded.bytes(e);
    if (needed >= 0) {
      flags.markBudget(
          new DecodeFlags.BudgetExceeded(
              -1, LimitKind.MAX_CODEC_MEMORY_BYTES, memoryLimit, needed, new byte[0]));
      return new IOException(name + " needs " + needed + " bytes of memory to decompress", e);
    }
    String detail =
        e instanceof EOFException || e.getMessage() == null
            ? "unexpected end of data"
            : e.getMessage();
    CorruptCompressionException corrupt =
        new CorruptCompressionException(name + " data is corrupt: " + detail, e);
    flags.markDecompress(corrupt);
    return corrupt;
  }

  @Override
  public void close() throws IOException {
    if (library != null) {
      library.close();
    } else {
      in.close();
    }
  }
}
