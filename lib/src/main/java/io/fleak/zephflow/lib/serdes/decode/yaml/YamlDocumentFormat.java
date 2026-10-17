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
package io.fleak.zephflow.lib.serdes.decode.yaml;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/** The whole input is one YAML document and one record. An empty or null document gives none. */
public final class YamlDocumentFormat implements FormatCursor {

  private final InputStream in;
  private final long maxDocumentBytes;
  private boolean done;

  public YamlDocumentFormat(InputStream in, DecodeLimits limits) {
    this.in = in;
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
      case WholeInputReader.Whole whole -> parse(whole.bytes());
    };
  }

  private DecodeOutcome parse(byte[] bytes) {
    RawSpan span = new RawSpan(0, bytes.length);
    try {
      Object document = YamlSupport.yaml().load(new ByteArrayInputStream(bytes));
      return document == null
          ? null
          : new Decoded(YamlRecords.toRecord(document, maxDocumentBytes), 1, span);
    } catch (MalformedYamlException | RuntimeException e) {
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
