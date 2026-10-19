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
package io.fleak.zephflow.lib.serdes.decode.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.Previews;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Messages one after another, each preceded by its length as a varint, as writeDelimitedTo writes
 * them. The length prefix frames each message, so one that does not parse or is over {@code
 * maxRecordBytes} loses only itself; a broken prefix or an input that ends inside a message is
 * terminal. A record's span and raw bytes are the message, without its prefix.
 */
public final class ProtobufDelimitedFormat implements FormatCursor {

  /** The largest message a byte array can hold. */
  private static final long MAX_MESSAGE = Integer.MAX_VALUE - 8;

  private final InputStream in;
  private final Descriptor type;
  private final long maxRecordBytes;
  private long offset;
  private long messageStart;
  private long index;

  public ProtobufDelimitedFormat(InputStream in, DecodeLimits limits, Descriptor type) {
    this.in = new BufferedInputStream(in, 64 * 1024);
    this.type = type;
    this.maxRecordBytes = limits.maxRecordBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    messageStart = offset;
    int first = read();
    if (first < 0) {
      return null;
    }
    long length = readLength(first);
    long recordIndex = index + 1;
    long bodyStart = offset;
    if ((maxRecordBytes > 0 && length > maxRecordBytes) || length > MAX_MESSAGE) {
      byte[] preview = skip(length, bodyStart);
      index = recordIndex;
      return new Oversized(recordIndex, new RawSpan(bodyStart, length), length, preview, false);
    }
    byte[] bytes = readFully((int) length);
    index = recordIndex;
    RawSpan span = new RawSpan(bodyStart, length);
    try {
      return new Decoded(ProtobufRecords.parse(type, bytes), recordIndex, span);
    } catch (IOException | RuntimeException e) {
      return new Failed(ErrorKind.MALFORMED, bytes, recordIndex, span, e, false);
    }
  }

  /** A base-128 varint of at most 5 bytes, like CodedInputStream's 32-bit lengths. */
  private long readLength(int first) throws IOException {
    long length = 0;
    int b = first;
    for (int shift = 0; shift < 35; shift += 7) {
      if (b < 0) {
        throw new MalformedProtobufException(
            "input ends inside the length of message " + (index + 1));
      }
      length |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        if (length > Integer.MAX_VALUE) {
          throw new MalformedProtobufException(
              "length of message " + (index + 1) + " is out of range");
        }
        return length;
      }
      b = read();
    }
    throw new MalformedProtobufException(
        "length of message " + (index + 1) + " is longer than 5 bytes");
  }

  private byte[] readFully(int length) throws IOException {
    byte[] bytes = in.readNBytes(length);
    offset += bytes.length;
    if (bytes.length < length) {
      throw new MalformedProtobufException("input ends inside message " + (index + 1));
    }
    return bytes;
  }

  /** Reads past an oversized message, keeping its first bytes as a preview. */
  private byte[] skip(long length, long bodyStart) throws IOException {
    byte[] preview = in.readNBytes((int) Math.min(length, Previews.PREVIEW_BYTES));
    offset += preview.length;
    byte[] scratch = new byte[64 * 1024];
    long left = length - preview.length;
    while (left > 0) {
      int n = in.read(scratch, 0, (int) Math.min(left, scratch.length));
      if (n < 0) {
        break;
      }
      offset += n;
      left -= n;
    }
    if (offset - bodyStart < length) {
      throw new MalformedProtobufException("input ends inside message " + (index + 1));
    }
    return preview;
  }

  private int read() throws IOException {
    int b = in.read();
    if (b >= 0) {
      offset++;
    }
    return b;
  }

  @Override
  public long pendingRecordIndex() {
    return index + 1;
  }

  @Override
  public long pendingOffset() {
    return messageStart;
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
