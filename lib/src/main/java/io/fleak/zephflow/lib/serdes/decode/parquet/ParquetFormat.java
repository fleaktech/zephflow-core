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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/**
 * A parquet file, one row group at a time, read by parquet's own {@link ParquetFileReader}. Before
 * it parses the footer, the footer is checked ({@link ParquetFooter}); before it reads a row group,
 * the row group's column chunks must lie between the leading magic and the footer, and twice its
 * stored size (it is held twice while read) and its declared uncompressed size must each fit {@code
 * maxDocumentBytes} (without it, half the heap, shared with the footer); its chunks are then read
 * once, their page headers checked ({@link PageHeaderScan}) and the bytes lent to parquet. Pages
 * are decompressed by {@link PageCodecs}, into exactly their claimed size; their contents are not
 * checked, since inputs are trusted not to be crafted. A footer that does not parse or does not fit
 * is terminal. A row group that does not fit is skipped unread as {@code Oversized}; one that is
 * damaged loses its remaining rows as one {@code Failed}; decoding then goes on with the next row
 * group. A row over {@code maxRecordBytes}, or one whose value does not convert, loses only itself.
 * Records have no raw bytes and no span.
 */
public final class ParquetFormat implements FormatCursor {

  private static final byte[] EMPTY = new byte[0];

  /**
   * Without {@code maxRecordBytes} a row may take this share of the budget. A row takes up to about
   * {@value #ROW_MULTIPLE} times its estimate, which without {@code maxDocumentBytes} is reserved
   * out of the row group's share so that the two together stay within the budget.
   */
  private static final long ROW_SHARE = 64;

  private static final long ROW_MULTIPLE = 16;

  private final RandomAccessInput in;
  private final DecodeLimits limits;
  private final DecodeFlags flags;
  private final long budget;
  private long rowGroupBudget;
  private long metadataStart;
  private RangeInputFile file;
  private ParquetFileReader reader;
  private List<BlockMetaData> groups;
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
    if (reader == null) {
      ParquetFooter footer;
      try {
        footer = ParquetFooter.check(in, budget);
      } catch (TooLargeException e) {
        return new Oversized(nextIndex, null, e.size, EMPTY, true);
      }
      metadataStart = footer.metadataStart();
      long rowLimit = limits.maxRecordBytes() > 0 ? limits.maxRecordBytes() : budget / ROW_SHARE;
      rowGroupBudget =
          limits.maxDocumentBytes() > 0
              ? budget
              : Math.max(
                  0,
                  budget
                      - footer.heap()
                      - (limits.maxRecordBytes() > 0 ? 0 : ROW_MULTIPLE * rowLimit));
      if (limits.maxDocumentBytes() == 0 && rowGroupBudget < budget / 4) {
        return new Oversized(nextIndex, null, footer.heap(), EMPTY, true);
      }
      ParquetReadOptions options =
          ParquetReadOptions.builder()
              .useStatsFilter(false)
              .useDictionaryFilter(false)
              .useRecordFilter(false)
              .useColumnIndexFilter(false)
              .useBloomFilter(false)
              .withCodecFactory(new PageCodecs(rowGroupBudget))
              .build();
      file = new RangeInputFile(in);
      reader = new ParquetFileReader(file, options);
      groups = reader.getRowGroups();
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      columns = new ColumnIOFactory().getColumnIO(schema);
      records = new ParquetRecords(schema, rowLimit);
    }
    while (true) {
      if (nextIndex < groupEnd) {
        return nextRow();
      }
      if (nextGroup >= groups.size()) {
        return null;
      }
      BlockMetaData group = groups.get(nextGroup++);
      long rowCount = group.getRowCount();
      rows = null;
      if (rowCount <= 0 || rowCount > Long.MAX_VALUE - nextIndex) {
        reader.skipNextRowGroup();
        if (rowCount == 0) {
          continue;
        }
        groupEnd = nextIndex + 1;
        return skipGroup(
            groupFailure(
                new MalformedParquetException("parquet row group claims " + rowCount + " rows")));
      }
      groupEnd = nextIndex + rowCount;
      String misplaced = misplacedChunk(group);
      if (misplaced != null) {
        reader.skipNextRowGroup();
        return skipGroup(groupFailure(new MalformedParquetException(misplaced)));
      }
      long size = Math.max(2 * group.getCompressedSize(), group.getTotalByteSize());
      if (size > rowGroupBudget) {
        reader.skipNextRowGroup();
        return skipGroup(new Oversized(nextIndex, null, size, EMPTY, false));
      }
      PageReadStore pages;
      try {
        lendCheckedChunks(group);
        pages = reader.readNextRowGroup();
      } catch (IOException | RuntimeException e) {
        reader.skipNextRowGroup();
        return skipGroup(groupFailure(e));
      } finally {
        file.reclaim();
      }
      try {
        rows = columns.getRecordReader(pages, records);
      } catch (RuntimeException e) {
        return skipGroup(groupFailure(e));
      }
    }
  }

  /**
   * Reads the row group's chunks, in runs of consecutive chunks as parquet does, checks their page
   * headers, and lends the bytes to parquet's reader so that it reads them from memory.
   */
  private void lendCheckedChunks(BlockMetaData group) throws IOException {
    List<ColumnChunkMetaData> chunks = new ArrayList<>(group.getColumns());
    chunks.sort(Comparator.comparingLong(ColumnChunkMetaData::getStartingPos));
    int first = 0;
    while (first < chunks.size()) {
      long start = chunks.get(first).getStartingPos();
      long end = start + chunks.get(first).getTotalSize();
      int last = first + 1;
      while (last < chunks.size() && chunks.get(last).getStartingPos() == end) {
        end += chunks.get(last).getTotalSize();
        last++;
      }
      byte[] bytes = ParquetFooter.bytes(in, start, (int) (end - start));
      for (ColumnChunkMetaData chunk : chunks.subList(first, last)) {
        PageHeaderScan.check(
            bytes,
            (int) (chunk.getStartingPos() - start),
            (int) chunk.getTotalSize(),
            chunk.getStartingPos(),
            metadataStart,
            chunk.getValueCount(),
            rowGroupBudget,
            chunk.getPath().toString());
      }
      file.lend(start, bytes);
      first = last;
    }
  }

  /** Why a column chunk lies outside the data, or null. */
  private String misplacedChunk(BlockMetaData group) {
    for (ColumnChunkMetaData chunk : group.getColumns()) {
      long start = chunk.getStartingPos();
      long size = chunk.getTotalSize();
      if (start < 4 || size < 0 || size > metadataStart - start) {
        return "parquet column chunk "
            + chunk.getPath()
            + " of "
            + size
            + " bytes at "
            + start
            + " is outside the data";
      }
    }
    return null;
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
    return new Failed(ErrorKind.MALFORMED, EMPTY, nextIndex, null, ownCause(e), false);
  }

  /** Our own reason when parquet wrapped it, such as a page that does not decompress. */
  private static Exception ownCause(Exception e) {
    for (Throwable cause = e; cause != null; cause = cause.getCause()) {
      if (cause instanceof MalformedParquetException own) {
        return own;
      }
    }
    return e;
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
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException ignored) {
        // Closing after the bytes were consumed cannot lose data.
      }
    }
  }
}
