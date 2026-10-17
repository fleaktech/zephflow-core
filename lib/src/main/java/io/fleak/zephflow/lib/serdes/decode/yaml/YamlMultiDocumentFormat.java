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
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.internal.BudgetCaptureStream;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.yaml.snakeyaml.constructor.ConstructorException;

/**
 * One record per document. A document that is not a mapping, or that fails to construct (duplicate
 * keys, unknown tags, an alias loop), is malformed and decoding goes on; a syntax error is
 * terminal, because the parser cannot find the next document. Empty and null documents give no
 * record. The parser reads ahead, so documents cannot be located: no span, no raw bytes, no
 * preview.
 */
public final class YamlMultiDocumentFormat implements FormatCursor {

  private static final byte[] EMPTY = new byte[0];
  private static final int PARSER_BUFFER = 16 * 1024;

  private final InputStream in;
  private final long maxRecordBytes;
  private final DecodeFlags flags;
  private BudgetCaptureStream budget;
  private Iterator<Object> documents;
  private long index;

  public YamlMultiDocumentFormat(InputStream in, DecodeLimits limits, DecodeFlags flags) {
    this.in = in;
    this.maxRecordBytes = limits.maxRecordBytes();
    this.flags = flags;
  }

  @Override
  public DecodeOutcome next() throws MalformedYamlException {
    if (documents == null) {
      budget = new BudgetCaptureStream(in, maxRecordBytes, flags, false);
      documents =
          YamlSupport.yaml(YamlRecords.maxValues(recordSizeLimit())).loadAll(budget).iterator();
    }
    while (documents.hasNext()) {
      Object document;
      try {
        document = documents.next();
      } catch (ConstructorException e) {
        if (flags.anySet()) {
          throw e;
        }
        budget.startRecord(budget.position());
        return new Failed(ErrorKind.MALFORMED, EMPTY, ++index, null, e, false);
      }
      budget.startRecord(budget.position());
      if (document == null) {
        continue;
      }
      index++;
      try {
        return new Decoded(YamlRecords.toRecord(document, recordSizeLimit()), index, null);
      } catch (MalformedYamlException e) {
        return new Failed(ErrorKind.MALFORMED, EMPTY, index, null, e, false);
      }
    }
    return null;
  }

  /**
   * What the budget can let through for one document: the limit, its slack, and the parser's
   * buffer, which the budget does not see because it restarts at the bytes read so far.
   */
  private long recordSizeLimit() {
    return maxRecordBytes > 0
        ? maxRecordBytes + BudgetCaptureStream.READ_AHEAD_SLACK + PARSER_BUFFER
        : 0;
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
      in.close();
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
