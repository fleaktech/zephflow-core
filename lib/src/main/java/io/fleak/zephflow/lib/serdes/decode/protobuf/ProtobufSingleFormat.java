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
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import java.io.IOException;
import java.io.InputStream;

/** The whole input is one message; an empty input is a message with every field at its default. */
public final class ProtobufSingleFormat implements FormatCursor {

  private final InputStream in;
  private final Descriptor type;
  private final long maxDocumentBytes;
  private boolean done;

  public ProtobufSingleFormat(InputStream in, DecodeLimits limits, Descriptor type) {
    this.in = in;
    this.type = type;
    this.maxDocumentBytes = limits.maxDocumentBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (done) {
      return null;
    }
    done = true;
    return switch (WholeInputReader.read(in, maxDocumentBytes)) {
      case WholeInputReader.TooLarge tooLarge -> tooLarge.outcome();
      case WholeInputReader.Whole whole -> parse(whole.bytes());
    };
  }

  private DecodeOutcome parse(byte[] bytes) {
    RawSpan span = new RawSpan(0, bytes.length);
    try {
      return new Decoded(ProtobufRecords.parse(type, bytes), 1, span);
    } catch (IOException | RuntimeException e) {
      return new Failed(ErrorKind.MALFORMED, bytes, -1, span, e, true);
    }
  }

  @Override
  public long pendingRecordIndex() {
    return -1;
  }

  @Override
  public long pendingOffset() {
    return 0;
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
