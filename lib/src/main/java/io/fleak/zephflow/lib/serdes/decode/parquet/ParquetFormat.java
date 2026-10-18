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

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RandomAccessInput;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import java.io.IOException;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;

/**
 * A parquet file, one row group at a time, without parquet's file reader, which allocates whatever
 * the footer and page headers claim. The footer and each row group may take {@code
 * maxDocumentBytes} in memory; without it, they share half the heap. A footer that does not parse
 * or does not fit is terminal. A row group that does not fit is skipped undecoded as {@code
 * Oversized}; a damaged one loses its remaining rows as one {@code Failed}; decoding then goes on
 * with the next row group. A row over {@code maxRecordBytes}, or one whose value does not convert,
 * loses only itself. Records have no raw bytes and no span.
 */
public final class ParquetFormat implements FormatCursor {

  private static final byte[] EMPTY = new byte[0];

  /**
   * A row takes up to about 14 times its estimate, so without a limit it gets 1/16 of the budget.
   */
  private static final long ROW_SHARE = 16;

  private final RandomAccessInput in;
  private final DecodeLimits limits;
  private final DecodeFlags flags;
  private final long budget;
  private long rowGroupBudget;
  private ParquetFooter footer;
  private MessageColumnIO columns;
  private ParquetRecords records;
  private int nextGroup;
  private RecordReader<RecordFleakData> rows;
  private long groupEnd = 1;
  private long nextIndex = 1;

  public ParquetFormat(RandomAccessInput in, DecodeLimits limits, DecodeFlags flags) {
    this.in = in;
    this.limits = limits;
    this.flags = flags;
    this.budget =
        limits.maxDocumentBytes() > 0
            ? limits.maxDocumentBytes()
            : Runtime.getRuntime().maxMemory() / 2;
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (footer == null) {
      try {
        footer = ParquetFooter.read(in, budget);
      } catch (TooLargeException e) {
        return new Oversized(nextIndex, null, e.size, EMPTY, true);
      }
      columns = new ColumnIOFactory().getColumnIO(footer.schema());
      rowGroupBudget = limits.maxDocumentBytes() > 0 ? budget : Math.max(0, budget - footer.heap());
      records =
          new ParquetRecords(
              footer.schema(),
              limits.maxRecordBytes() > 0 ? limits.maxRecordBytes() : rowGroupBudget / ROW_SHARE);
    }
    while (true) {
      if (nextIndex < groupEnd) {
        return nextRow();
      }
      if (nextGroup >= footer.rowGroups().size()) {
        return null;
      }
      RowGroup group = footer.rowGroups().get(nextGroup++);
      long rowCount = group.getNum_rows();
      rows = null;
      if (rowCount < 0 || rowCount > Long.MAX_VALUE - nextIndex) {
        groupEnd = nextIndex + 1;
        return skipGroup(
            groupFailure(
                new MalformedParquetException("parquet row group claims " + rowCount + " rows")));
      }
      groupEnd = nextIndex + rowCount;
      if (rowCount == 0) {
        continue;
      }
      try {
        rows =
            columns.getRecordReader(RowGroupPages.read(in, footer, group, rowGroupBudget), records);
      } catch (RowGroupPages.RowCountException e) {
        groupEnd = nextIndex + 1;
        return skipGroup(groupFailure(new MalformedParquetException(e.getMessage())));
      } catch (TooLargeException e) {
        return skipGroup(new Oversized(nextIndex, null, e.size, EMPTY, false));
      } catch (IOException | RuntimeException e) {
        return skipGroup(groupFailure(e));
      }
    }
  }

  private DecodeOutcome nextRow() throws IOException {
    long index = nextIndex;
    try {
      rows.read();
    } catch (RuntimeException e) {
      return skipGroup(groupFailure(e));
    }
    nextIndex++;
    if (records.failure() != null) {
      return new Failed(ErrorKind.MALFORMED, EMPTY, index, null, records.failure(), false);
    }
    if (records.oversized()) {
      return new Oversized(index, null, records.size(), EMPTY, false);
    }
    return new Decoded(records.getCurrentRecord(), index, null);
  }

  /** The rest of the current row group is lost; its first lost row carries the outcome. */
  private DecodeOutcome skipGroup(DecodeOutcome outcome) {
    rows = null;
    nextIndex = groupEnd;
    return outcome;
  }

  private Failed groupFailure(Exception e) throws IOException {
    if (flags.anySet()) {
      throw e instanceof IOException io ? io : new IOException(e);
    }
    Exception cause =
        e instanceof RowGroupPages.PageException page && page.getCause() instanceof IOException io
            ? io
            : e;
    return new Failed(ErrorKind.MALFORMED, EMPTY, nextIndex, null, cause, false);
  }

  @Override
  public long pendingRecordIndex() {
    return nextIndex;
  }

  @Override
  public long pendingOffset() {
    return -1;
  }

  @Override
  public void close() {
    rows = null;
  }
}
