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
package io.fleak.zephflow.lib.serdes.decode.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.BudgetCaptureStream;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import java.io.IOException;
import java.io.InputStream;

/** Streams the elements of a top-level array, one element in memory at a time. */
public final class JsonArrayFormat implements FormatCursor {

  private static final byte[] EMPTY = new byte[0];

  private final InputStream in;
  private final DecodeLimits limits;
  private final DecodeFlags flags;
  private BudgetCaptureStream budget;
  private JsonParser parser;
  private long index;
  private long pendingOffset;
  private boolean done;

  public JsonArrayFormat(InputStream in, DecodeLimits limits, DecodeFlags flags) {
    this.in = in;
    this.limits = limits;
    this.flags = flags;
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (done) {
      return null;
    }
    if (parser == null) {
      budget = new BudgetCaptureStream(JsonEncoding.toUtf8(in), limits.maxRecordBytes(), flags);
      parser = JsonDecoding.FACTORY.createParser(budget);
      JsonToken first = parser.nextToken();
      if (first == null) {
        if (budget.position() == 0) {
          done = true;
          return null;
        }
        return terminal("no JSON array in the input");
      }
      if (first != JsonToken.START_ARRAY) {
        return terminal("expected a JSON array, got " + JsonDecoding.describe(first));
      }
      pendingOffset = JsonDecoding.byteOffset(parser.currentLocation());
    }
    JsonToken token = parser.nextToken();
    if (token == JsonToken.END_ARRAY) {
      done = true;
      if (parser.nextToken() != null) {
        return terminal("unexpected content after the JSON array");
      }
      return null;
    }
    if (token == null) {
      return terminal("the JSON array is truncated");
    }
    long start = JsonDecoding.byteOffset(parser.currentTokenLocation());
    pendingOffset = start;
    budget.startRecord(start);
    JsonNode node = JsonDecoding.MAPPER.readTree(parser);
    long end = JsonDecoding.byteOffset(parser.currentLocation());
    long recordIndex = ++index;
    RawSpan span = new RawSpan(start, end - start);
    DecodeOutcome outcome;
    try {
      outcome = new Decoded(JsonDecoding.toRecord(node), recordIndex, span);
    } catch (RuntimeException e) {
      outcome =
          new Failed(ErrorKind.MALFORMED, budget.bytes(start, end), recordIndex, span, e, false);
    }
    pendingOffset = end;
    budget.startRecord(end);
    return outcome;
  }

  private Failed terminal(String message) {
    done = true;
    return new Failed(
        ErrorKind.MALFORMED,
        EMPTY,
        index + 1,
        RawSpan.toEnd(Math.max(pendingOffset, 0)),
        new IllegalArgumentException(message),
        true);
  }

  @Override
  public long pendingRecordIndex() {
    return index + 1;
  }

  @Override
  public long pendingOffset() {
    return pendingOffset;
  }

  @Override
  public void close() {
    try {
      if (parser != null) {
        parser.close();
      }
      if (budget != null) {
        budget.close();
      } else {
        in.close();
      }
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
