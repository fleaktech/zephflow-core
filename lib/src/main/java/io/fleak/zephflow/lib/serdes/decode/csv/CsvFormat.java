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
package io.fleak.zephflow.lib.serdes.decode.csv;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvScanner.Frame;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvScanner.OversizedRow;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvScanner.Row;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvScanner.UnclosedQuote;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.Transcoding;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One record per CSV row, keyed by column name; every value is a string. Header problems are
 * terminal and reported with recordIndex 0; data rows are numbered from 1.
 */
public final class CsvFormat implements FormatCursor {

  private final InputStream in;
  private final CsvDialect dialect;
  private final long maxRecordBytes;
  private CsvScanner scanner;
  private List<String> names;
  private boolean headerPending;
  private long index;

  public CsvFormat(InputStream in, DecodeLimits limits, CsvDialect dialect) {
    this.in = in;
    this.dialect = dialect;
    this.maxRecordBytes = limits.maxRecordBytes();
    this.names = dialect.columns();
    this.headerPending = dialect.header();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (scanner == null) {
      InputStream utf8 = Transcoding.toUtf8(in, dialect.charset());
      scanner = new CsvScanner(utf8, dialect, maxRecordBytes, utf8 == in);
      if (headerPending) {
        DecodeOutcome problem = header(scanner.next());
        headerPending = false;
        if (problem != null) {
          return problem;
        }
      }
    }
    Frame frame = scanner.next();
    if (frame == null) {
      return null;
    }
    index++;
    return switch (frame) {
      case OversizedRow o -> oversized(o, index);
      case UnclosedQuote u -> unclosed(u, index);
      case Row row -> row(row);
    };
  }

  /** Null when the header is fine (or the input is empty). */
  private DecodeOutcome header(Frame frame) {
    return switch (frame) {
      case null -> null;
      case OversizedRow o when names != null && !o.terminal() -> null;
      case OversizedRow o ->
          oversized(new OversizedRow(o.offset(), o.size(), o.preview(), true), 0);
      case UnclosedQuote u -> unclosed(u, 0);
      case Row row when names != null -> null;
      case Row row -> {
        if (row.problem() != null) {
          yield headerFailed(row, "header: " + row.problem());
        }
        List<String> fromHeader = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < row.fields().size(); i++) {
          String name = new String(row.fields().get(i), StandardCharsets.UTF_8);
          if (name.isEmpty()) {
            name = generatedName(i);
          }
          if (!seen.add(name)) {
            yield headerFailed(row, "header has a duplicate column name: " + name);
          }
          fromHeader.add(name);
        }
        names = List.copyOf(fromHeader);
        yield null;
      }
    };
  }

  private DecodeOutcome row(Row row) {
    if (row.problem() != null) {
      return failed(row, index, row.problem());
    }
    List<byte[]> fields = row.fields();
    if (names != null && fields.size() != names.size()) {
      return failed(row, index, "row has " + fields.size() + " columns, expected " + names.size());
    }
    Map<String, FleakData> payload = new HashMap<>();
    for (int i = 0; i < fields.size(); i++) {
      payload.put(
          names != null ? names.get(i) : generatedName(i),
          new StringPrimitiveFleakData(new String(fields.get(i), StandardCharsets.UTF_8)));
    }
    return new Decoded(new RecordFleakData(payload), index, span(row));
  }

  private static String generatedName(int i) {
    return "_c" + i;
  }

  private static RawSpan span(Row row) {
    return new RawSpan(row.offset(), row.bytes().length);
  }

  private static Failed headerFailed(Row row, String message) {
    return new Failed(
        ErrorKind.MALFORMED,
        row.bytes(),
        0,
        RawSpan.toEnd(row.offset()),
        new MalformedCsvException(message),
        true);
  }

  private static Failed failed(Row row, long recordIndex, String message) {
    return new Failed(
        ErrorKind.MALFORMED,
        row.bytes(),
        recordIndex,
        span(row),
        new MalformedCsvException(message),
        false);
  }

  private Oversized oversized(OversizedRow o, long recordIndex) {
    RawSpan span = o.terminal() ? RawSpan.toEnd(o.offset()) : new RawSpan(o.offset(), o.size());
    return new Oversized(
        recordIndex,
        1,
        span,
        LimitKind.MAX_RECORD_BYTES,
        maxRecordBytes,
        o.size(),
        o.preview(),
        o.terminal());
  }

  private static Failed unclosed(UnclosedQuote u, long recordIndex) {
    return new Failed(
        ErrorKind.MALFORMED,
        u.bytes(),
        recordIndex,
        RawSpan.toEnd(u.offset()),
        new MalformedCsvException("quoted field is not closed before the end of the input"),
        true);
  }

  @Override
  public long pendingRecordIndex() {
    return headerPending ? 0 : index + 1;
  }

  @Override
  public long pendingOffset() {
    return scanner == null ? 0 : scanner.pendingOffset();
  }

  @Override
  public void close() {
    if (scanner != null) {
      scanner.close();
      return;
    }
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing was read.
    }
  }
}
