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
package io.fleak.zephflow.lib.serdes.decode.xml;

import io.fleak.zephflow.api.structure.RecordFleakData;
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
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/** The whole input is one XML document and one record; anything after the root is an error. */
public final class XmlDocumentFormat implements FormatCursor {

  private final InputStream in;
  private final long maxDocumentBytes;
  private boolean done;

  public XmlDocumentFormat(InputStream in, DecodeLimits limits) {
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

  private static DecodeOutcome parse(byte[] bytes) {
    if (bytes.length == 0) {
      return null;
    }
    RawSpan span = new RawSpan(0, bytes.length);
    try {
      return new Decoded(parseDocument(bytes), 1, span);
    } catch (IOException | XMLStreamException | RuntimeException e) {
      return new Failed(ErrorKind.MALFORMED, bytes, -1, span, e, true);
    }
  }

  private static RecordFleakData parseDocument(byte[] bytes)
      throws IOException, XMLStreamException {
    XMLStreamReader reader = XmlSupport.reader(new ByteArrayInputStream(bytes));
    try {
      int event;
      while ((event = reader.next()) != XMLStreamConstants.START_ELEMENT) {
        XmlSupport.rejectDtd(event);
      }
      RecordFleakData record = XmlSupport.readElement(reader);
      while (reader.hasNext()) {
        event = reader.next();
        XmlSupport.rejectDtd(event);
        if (event == XMLStreamConstants.START_ELEMENT
            || (event == XMLStreamConstants.CHARACTERS && !reader.isWhiteSpace())) {
          throw new MalformedXmlException("unexpected content after the root element");
        }
      }
      return record;
    } finally {
      reader.close();
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
