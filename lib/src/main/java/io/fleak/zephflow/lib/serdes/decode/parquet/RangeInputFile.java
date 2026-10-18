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
package io.fleak.zephflow.lib.serdes.decode.parquet;

import io.fleak.zephflow.lib.serdes.decode.RandomAccessInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * parquet's view of a {@link RandomAccessInput}. Each read asks for one range; reads under {@value
 * #BUFFER} bytes are served from a range of that size, since parquet reads lengths and magic bytes
 * a few at a time. Ranges already read for a row group can be lent to it, so that parquet reads
 * them from memory instead of asking again.
 */
final class RangeInputFile implements InputFile {

  private static final int BUFFER = 8192;

  private final RandomAccessInput in;
  private final List<Range> lent = new ArrayList<>();

  private record Range(long start, byte[] bytes) {
    boolean holds(long offset, int length) {
      return offset >= start && offset + length <= start + bytes.length;
    }
  }

  RangeInputFile(RandomAccessInput in) {
    this.in = in;
  }

  void lend(long start, byte[] bytes) {
    lent.add(new Range(start, bytes));
  }

  void reclaim() {
    lent.clear();
  }

  @Override
  public long getLength() throws IOException {
    return in.length();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new Stream(in.length());
  }

  private final class Stream extends SeekableInputStream {

    private final long length;
    private final byte[] one = new byte[1];
    private long pos;
    private byte[] buffer = new byte[0];
    private long bufferStart;

    Stream(long length) {
      this.length = length;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > length) {
        throw new EOFException("seek to " + newPos + " outside the input of " + length + " bytes");
      }
      pos = newPos;
    }

    @Override
    public int read() throws IOException {
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (pos >= length) {
        return -1;
      }
      int n = (int) Math.min(len, length - pos);
      if (n >= BUFFER || isLent(pos, n)) {
        fetch(pos, b, off, n);
      } else {
        if (pos < bufferStart || pos + n > bufferStart + buffer.length) {
          bufferStart = pos;
          buffer = new byte[(int) Math.min(BUFFER, length - pos)];
          fetch(bufferStart, buffer, 0, buffer.length);
        }
        System.arraycopy(buffer, (int) (pos - bufferStart), b, off, n);
      }
      pos += n;
      return n;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
      readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
      if (len > length - pos) {
        throw new EOFException(
            "reading " + len + " bytes at " + pos + " runs past the input of " + length);
      }
      int done = 0;
      while (done < len) {
        done += read(bytes, start + done, len - done);
      }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
      int len = (int) Math.min(buf.remaining(), length - pos);
      if (len == 0) {
        return buf.hasRemaining() ? -1 : 0;
      }
      readFully(buf, len);
      return len;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
      readFully(buf, buf.remaining());
    }

    private void readFully(ByteBuffer buf, int len) throws IOException {
      if (buf.hasArray()) {
        readFully(buf.array(), buf.arrayOffset() + buf.position(), len);
        buf.position(buf.position() + len);
      } else {
        byte[] bytes = new byte[len];
        readFully(bytes, 0, len);
        buf.put(bytes);
      }
    }

    private boolean isLent(long offset, int len) {
      return lent.stream().anyMatch(range -> range.holds(offset, len));
    }

    private void fetch(long offset, byte[] b, int off, int len) throws IOException {
      for (Range range : lent) {
        if (range.holds(offset, len)) {
          System.arraycopy(range.bytes(), (int) (offset - range.start()), b, off, len);
          return;
        }
      }
      try (InputStream range = in.read(offset, len)) {
        range.readNBytes(b, off, len);
      }
    }
  }
}
