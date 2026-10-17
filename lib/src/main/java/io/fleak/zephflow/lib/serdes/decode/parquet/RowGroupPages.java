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

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.RandomAccessInput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.schema.PrimitiveType;

/**
 * One row group, read whole and checked before parquet assembles a record from it. Every column
 * chunk must lie between the leading magic and the footer, and together they may take {@code
 * maxDocumentBytes} both as stored and as their pages claim to decompress, where a delta-encoded
 * page also counts the 8 bytes per value (16 for DELTA_BYTE_ARRAY) that parquet's decoder holds for
 * the chunk's largest page. Every page header is read first. A column that does not repeat has
 * exactly one value per row; repeated columns may add at most {@value #ITEMS_PER_BYTE} list items
 * per decompressed byte plus {@value #EXTRA_ITEMS}, because levels can claim items in no bytes.
 * Each page is then decompressed and checked when parquet asks for it.
 */
final class RowGroupPages implements PageReadStore {

  static final long ITEMS_PER_BYTE = 32;
  static final long EXTRA_ITEMS = 1_000_000;

  private static final ParquetMetadataConverter CONVERTER = new ParquetMetadataConverter();

  private final long rows;
  private final Map<List<String>, ChunkPages> chunks;

  private RowGroupPages(long rows, Map<List<String>, ChunkPages> chunks) {
    this.rows = rows;
    this.chunks = chunks;
  }

  /** A row group over {@code maxDocumentBytes}; nothing of it is decompressed. */
  static final class TooLargeException extends IOException {
    final long size;

    TooLargeException(long size) {
      super("parquet row group of " + size + " bytes is over maxDocumentBytes");
      this.size = size;
    }
  }

  static RowGroupPages read(
      RandomAccessInput in, ParquetFooter footer, RowGroup group, DecodeLimits limits)
      throws IOException {
    List<ColumnDescriptor> columns = footer.schema().getColumns();
    Map<List<String>, ColumnDescriptor> byPath = new HashMap<>();
    for (ColumnDescriptor column : columns) {
      byPath.put(List.of(column.getPath()), column);
    }
    List<ColumnChunk> chunkList = group.getColumns();
    if (chunkList == null || chunkList.size() != columns.size()) {
      throw new MalformedParquetException(
          "parquet row group has "
              + (chunkList == null ? 0 : chunkList.size())
              + " column chunks for "
              + columns.size()
              + " columns");
    }
    long limit = limits.maxDocumentBytes();
    long stored = 0;
    Map<List<String>, ColumnMetaData> metadata = new HashMap<>();
    for (ColumnChunk chunk : chunkList) {
      ColumnMetaData meta = chunk.getMeta_data();
      if (meta == null || chunk.isSetFile_path()) {
        throw new MalformedParquetException(
            "parquet column chunk in another file is not supported");
      }
      List<String> path =
          meta.getPath_in_schema() == null ? List.of() : List.copyOf(meta.getPath_in_schema());
      if (!byPath.containsKey(path) || metadata.put(path, meta) != null) {
        throw new MalformedParquetException(
            "parquet column chunk " + path + " is unknown or repeated");
      }
      long start = start(meta);
      long size = meta.getTotal_compressed_size();
      if (start < 4
          || size <= 0
          || size > footer.metadataStart() - start
          || meta.getNum_values() <= 0) {
        throw new MalformedParquetException(
            "parquet column chunk "
                + path
                + " of "
                + size
                + " bytes at "
                + start
                + " is outside the data");
      }
      stored += size;
    }
    if ((limit > 0 && stored > limit) || stored > Integer.MAX_VALUE - 8) {
      throw new TooLargeException(stored);
    }
    long rows = group.getNum_rows();
    Map<List<String>, ChunkPages> chunks = new HashMap<>();
    long decompressed = 0;
    long decoded = 0;
    long items = 0;
    for (Map.Entry<List<String>, ColumnMetaData> entry : metadata.entrySet()) {
      ColumnMetaData meta = entry.getValue();
      ColumnDescriptor column = byPath.get(entry.getKey());
      byte[] bytes = ParquetFooter.bytes(in, start(meta), (int) meta.getTotal_compressed_size());
      ChunkPages pages = ChunkPages.scan(column, meta, bytes);
      if (pages.values < rows || (column.getMaxRepetitionLevel() == 0 && pages.values != rows)) {
        throw new MalformedParquetException(
            "parquet column chunk "
                + entry.getKey()
                + " has "
                + pages.values
                + " values for "
                + rows
                + " rows");
      }
      items += pages.values - rows;
      decompressed += pages.decompressed;
      decoded += pages.deltaPeak;
      chunks.put(entry.getKey(), pages);
    }
    if (limit > 0 && decompressed + decoded > limit) {
      throw new TooLargeException(decompressed + decoded);
    }
    if (items > ITEMS_PER_BYTE * decompressed + EXTRA_ITEMS) {
      throw new MalformedParquetException(
          "parquet row group claims "
              + items
              + " list items in "
              + decompressed
              + " bytes, more than "
              + ITEMS_PER_BYTE
              + " per byte plus "
              + EXTRA_ITEMS);
    }
    return new RowGroupPages(rows, chunks);
  }

  /** The dictionary page when it comes first, as parquet-mr decides. */
  private static long start(ColumnMetaData meta) {
    long data = meta.getData_page_offset();
    long dictionary = meta.isSetDictionary_page_offset() ? meta.getDictionary_page_offset() : 0;
    return dictionary > 0 && dictionary < data ? dictionary : data;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor column) {
    ChunkPages pages = chunks.get(List.of(column.getPath()));
    if (pages == null) {
      throw new IllegalArgumentException(
          "no column chunk for " + Arrays.toString(column.getPath()));
    }
    return pages;
  }

  @Override
  public long getRowCount() {
    return rows;
  }

  private record Page(PageHeader header, int offset) {}

  private static final class ChunkPages implements PageReader {

    private final ColumnDescriptor column;
    private final PrimitiveType type;
    private final CompressionCodec codec;
    private final byte[] chunk;
    private final long totalValues;
    private final Page dictionary;
    private final Deque<Page> data;
    private long decompressed;
    private long values;
    private long deltaPeak;

    private ChunkPages(
        ColumnDescriptor column,
        ColumnMetaData meta,
        byte[] chunk,
        Page dictionary,
        Deque<Page> data) {
      this.column = column;
      this.type = column.getPrimitiveType();
      this.codec = meta.getCodec();
      this.chunk = chunk;
      this.totalValues = meta.getNum_values();
      this.dictionary = dictionary;
      this.data = data;
    }

    /** Reads every page header until the chunk's values are reached. */
    static ChunkPages scan(ColumnDescriptor column, ColumnMetaData meta, byte[] chunk)
        throws MalformedParquetException {
      Page dictionary = null;
      Deque<Page> data = new ArrayDeque<>();
      long decompressed = 0;
      long values = 0;
      long deltaPeak = 0;
      int pos = 0;
      while (values < meta.getNum_values()) {
        if (pos >= chunk.length) {
          throw new MalformedParquetException(
              "parquet column chunk ends after "
                  + values
                  + " of "
                  + meta.getNum_values()
                  + " values");
        }
        BoundedThrift.Header read = BoundedThrift.pageHeader(chunk, pos, chunk.length);
        PageHeader header = read.header();
        pos += read.length();
        int compressed = header.getCompressed_page_size();
        int uncompressed = header.getUncompressed_page_size();
        if (compressed < 0 || compressed > chunk.length - pos || uncompressed < 0) {
          throw new MalformedParquetException(
              "parquet page of " + compressed + " bytes does not fit its column chunk");
        }
        Page page = new Page(header, pos);
        switch (header.getType()) {
          case DICTIONARY_PAGE -> {
            DictionaryPageHeader dict = header.getDictionary_page_header();
            if (dict == null || dictionary != null || !data.isEmpty() || dict.getNum_values() < 0) {
              throw new MalformedParquetException(
                  "parquet dictionary page is invalid or misplaced");
            }
            dictionary = page;
          }
          case DATA_PAGE -> {
            DataPageHeader v1 = header.getData_page_header();
            if (v1 == null || v1.getNum_values() < 0) {
              throw new MalformedParquetException("parquet data page header is invalid");
            }
            values += v1.getNum_values();
            deltaPeak = Math.max(deltaPeak, deltaBytes(v1.getEncoding(), v1.getNum_values()));
            data.add(page);
          }
          case DATA_PAGE_V2 -> {
            DataPageHeaderV2 v2 = header.getData_page_header_v2();
            if (v2 == null
                || v2.getNum_values() < 0
                || v2.getNum_rows() < 0
                || v2.getNum_nulls() < 0
                || v2.getRepetition_levels_byte_length() < 0
                || v2.getDefinition_levels_byte_length() < 0
                || (long) v2.getRepetition_levels_byte_length()
                        + v2.getDefinition_levels_byte_length()
                    > Math.min(compressed, uncompressed)) {
              throw new MalformedParquetException("parquet data page v2 header is invalid");
            }
            values += v2.getNum_values();
            deltaPeak = Math.max(deltaPeak, deltaBytes(v2.getEncoding(), v2.getNum_values()));
            data.add(page);
          }
          default -> {}
        }
        decompressed += uncompressed;
        pos += compressed;
      }
      if (values != meta.getNum_values()) {
        throw new MalformedParquetException(
            "parquet column chunk has " + values + " values, not " + meta.getNum_values());
      }
      ChunkPages pages = new ChunkPages(column, meta, chunk, dictionary, data);
      pages.decompressed = decompressed;
      pages.values = values;
      pages.deltaPeak = deltaPeak;
      return pages;
    }

    /** What parquet's delta decoder holds for a page: every value, decoded up front. */
    private static long deltaBytes(org.apache.parquet.format.Encoding encoding, int values) {
      if (encoding == null) {
        return 0;
      }
      return switch (encoding) {
        case DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY -> 8L * values;
        case DELTA_BYTE_ARRAY -> 16L * values;
        default -> 0;
      };
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (dictionary == null) {
        return null;
      }
      try {
        DictionaryPageHeader header = dictionary.header().getDictionary_page_header();
        byte[] bytes = decompress(dictionary, dictionary.header().getUncompressed_page_size());
        PageChecks.dictionary(header.getNum_values(), bytes.length, type);
        return new DictionaryPage(
            BytesInput.from(bytes), header.getNum_values(), encoding(header.getEncoding()));
      } catch (MalformedParquetException e) {
        throw new PageException(e);
      }
    }

    @Override
    public long getTotalValueCount() {
      return totalValues;
    }

    @Override
    public DataPage readPage() {
      Page page = data.poll();
      if (page == null) {
        return null;
      }
      try {
        return page.header().isSetData_page_header() ? v1(page) : v2(page);
      } catch (IOException e) {
        throw new PageException(e);
      }
    }

    private DataPage v1(Page page) throws IOException {
      DataPageHeader header = page.header().getData_page_header();
      int count = header.getNum_values();
      int size = page.header().getUncompressed_page_size();
      byte[] bytes = decompress(page, size);
      Encoding repetition = encoding(header.getRepetition_level_encoding());
      Encoding definition = encoding(header.getDefinition_level_encoding());
      Encoding values = encoding(header.getEncoding());
      int pos = levels(bytes, 0, repetition, column.getMaxRepetitionLevel(), count);
      pos = levels(bytes, pos, definition, column.getMaxDefinitionLevel(), count);
      PageChecks.values(bytes, pos, bytes.length, values, count);
      return new DataPageV1(
          BytesInput.from(bytes),
          count,
          size,
          Statistics.getBuilderForReading(type).build(),
          repetition,
          definition,
          values);
    }

    private static int levels(byte[] page, int offset, Encoding encoding, int maxLevel, int count)
        throws MalformedParquetException {
      if (maxLevel == 0) {
        return offset;
      }
      int width = BytesUtils.getWidthFromMaxInt(maxLevel);
      return switch (encoding) {
        case RLE -> PageChecks.prefixedHybrid(page, offset, page.length, width, count);
        case BIT_PACKED -> {
          long bytes = ((long) count * width + 7) / 8;
          if (bytes > page.length - offset) {
            throw new MalformedParquetException("parquet bit-packed levels overflow the page");
          }
          yield offset + (int) bytes;
        }
        default -> throw new MalformedParquetException("parquet levels cannot be " + encoding);
      };
    }

    private DataPage v2(Page page) throws IOException {
      DataPageHeaderV2 header = page.header().getData_page_header_v2();
      int count = header.getNum_values();
      int repetitionLength = header.getRepetition_levels_byte_length();
      int definitionLength = header.getDefinition_levels_byte_length();
      int levels = repetitionLength + definitionLength;
      byte[] repetition =
          Arrays.copyOfRange(chunk, page.offset(), page.offset() + repetitionLength);
      byte[] definition =
          Arrays.copyOfRange(chunk, page.offset() + repetitionLength, page.offset() + levels);
      int stored = page.header().getCompressed_page_size() - levels;
      int size = page.header().getUncompressed_page_size() - levels;
      byte[] values =
          !header.isSetIs_compressed() || header.isIs_compressed()
              ? PageCodecs.decompress(codec, chunk, page.offset() + levels, stored, size)
              : PageCodecs.decompress(
                  CompressionCodec.UNCOMPRESSED, chunk, page.offset() + levels, stored, size);
      if (column.getMaxRepetitionLevel() > 0) {
        PageChecks.hybrid(
            repetition,
            0,
            repetition.length,
            BytesUtils.getWidthFromMaxInt(column.getMaxRepetitionLevel()),
            count);
      }
      if (column.getMaxDefinitionLevel() > 0) {
        PageChecks.hybrid(
            definition,
            0,
            definition.length,
            BytesUtils.getWidthFromMaxInt(column.getMaxDefinitionLevel()),
            count);
      }
      Encoding encoding = encoding(header.getEncoding());
      PageChecks.values(values, 0, values.length, encoding, count);
      return DataPageV2.uncompressed(
          header.getNum_rows(),
          header.getNum_nulls(),
          count,
          BytesInput.from(repetition),
          BytesInput.from(definition),
          encoding,
          BytesInput.from(values),
          Statistics.getBuilderForReading(type).build());
    }

    private byte[] decompress(Page page, int size) throws MalformedParquetException {
      return PageCodecs.decompress(
          codec, chunk, page.offset(), page.header().getCompressed_page_size(), size);
    }

    private static Encoding encoding(org.apache.parquet.format.Encoding encoding)
        throws MalformedParquetException {
      if (encoding == null) {
        throw new MalformedParquetException("parquet page has an unknown encoding");
      }
      return CONVERTER.getEncoding(encoding);
    }
  }

  /** Carries a page failure through parquet's column readers, which take no checked exception. */
  static final class PageException extends RuntimeException {
    PageException(IOException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
