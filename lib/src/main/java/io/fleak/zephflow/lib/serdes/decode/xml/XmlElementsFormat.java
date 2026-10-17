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
import io.fleak.zephflow.lib.serdes.decode.internal.BudgetCaptureStream;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * One record per element at {@code recordPath}, matched by local name with namespaces ignored. The
 * parser reads ahead, so records cannot be located: no span, no raw bytes, no preview. The budget
 * restarts after every event outside a record, so a huge element that is not a record is also
 * stopped.
 */
public final class XmlElementsFormat implements FormatCursor {

  private final InputStream in;
  private final List<String> recordPath;
  private final long maxRecordBytes;
  private final DecodeFlags flags;
  private final List<String> path = new ArrayList<>();
  private BudgetCaptureStream budget;
  private XMLStreamReader reader;
  private boolean sawRoot;
  private long index;

  public XmlElementsFormat(
      InputStream in, DecodeLimits limits, DecodeFlags flags, List<String> recordPath) {
    this.in = in;
    this.recordPath = recordPath;
    this.maxRecordBytes = limits.maxRecordBytes();
    this.flags = flags;
  }

  @Override
  public DecodeOutcome next() throws IOException, XMLStreamException {
    if (reader == null) {
      PeekedStream peeked = PeekedStream.peek(in, 1, head -> head.length > 0);
      if (peeked.head().length == 0) {
        return null;
      }
      budget = new BudgetCaptureStream(peeked, maxRecordBytes, flags, false);
      reader = XmlSupport.reader(budget);
    }
    while (reader.hasNext()) {
      int event = reader.next();
      XmlSupport.rejectDtd(event);
      if (event == XMLStreamConstants.START_ELEMENT) {
        sawRoot = true;
        path.add(reader.getLocalName());
        if (path.equals(recordPath)) {
          path.removeLast();
          RecordFleakData record = XmlSupport.readElement(reader);
          budget.startRecord(budget.position());
          return new Decoded(record, ++index, null);
        }
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        path.removeLast();
      }
      budget.startRecord(budget.position());
    }
    if (sawRoot && index == 0) {
      throw new MalformedXmlException(
          "recordPath /" + String.join("/", recordPath) + " matched no element");
    }
    return null;
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
      if (reader != null) {
        reader.close();
      }
    } catch (XMLStreamException ignored) {
      // Nothing left to read.
    }
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
