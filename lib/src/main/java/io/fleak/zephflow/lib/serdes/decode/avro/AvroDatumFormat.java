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
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

/**
 * The whole input is one avro datum written with the configured schema; bytes left after it are an
 * error. An empty input is decoded too, since a schema of only nulls encodes in zero bytes.
 */
public final class AvroDatumFormat implements FormatCursor {

  private final InputStream in;
  private final Schema schema;
  private final long maxDocumentBytes;
  private boolean done;

  public AvroDatumFormat(InputStream in, DecodeLimits limits, Schema schema) {
    this.in = in;
    this.schema = schema;
    this.maxDocumentBytes = limits.maxDocumentBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (done) {
      return null;
    }
    done = true;
    return switch (WholeInputReader.read(in, maxDocumentBytes)) {
      case WholeInputReader.TooLarge tooLarge ->
          new Oversized(-1, RawSpan.toEnd(0), tooLarge.sizeSoFar(), tooLarge.preview(), true);
      case WholeInputReader.Whole whole -> decode(whole.bytes());
    };
  }

  private DecodeOutcome decode(byte[] bytes) {
    RawSpan span = new RawSpan(0, bytes.length);
    try {
      BoundedDecoder decoder = new BoundedDecoder(bytes, 0, bytes.length);
      IndexedRecord record = new BoundedDatumReader(schema).read(decoder);
      if (!decoder.isEnd()) {
        throw new MalformedAvroException(
            decoder.remaining() + " bytes are left after the avro datum");
      }
      return new Decoded(AvroRecords.toRecord(record), 1, span);
    } catch (IOException | RuntimeException | StackOverflowError e) {
      return new Failed(ErrorKind.MALFORMED, bytes, -1, span, AvroOcfFormat.failure(e), true);
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
