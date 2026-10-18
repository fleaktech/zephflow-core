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

import io.fleak.zephflow.lib.serdes.decode.RandomAccessInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;

/**
 * Checks the file's footer before parquet's reader parses it, which allocates whatever its lists
 * and strings claim: the trailing length and magic, then the metadata, read with {@link
 * BoundedThrift}. What the footer holds in memory is estimated and must stay within the budget: its
 * thrift objects (see {@link BoundedThrift}), {@value #CHUNK_BYTES} bytes per column chunk for the
 * metadata parquet converts them into, and per leaf column {@value #LEAF_BYTES} bytes of schema,
 * column readers and converters plus {@value #CELL_BYTES} per cell of parquet's record reader
 * tables, which grow with each leaf's depth and levels. The schema is checked without recursion,
 * because parquet builds it recursively.
 */
record ParquetFooter(long metadataStart, long heap) {

  private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
  private static final int MAX_SCHEMA_DEPTH = 32;
  private static final long MAX_LEVEL_CELLS = 500_000;
  private static final long LEAF_BYTES = 2048;
  private static final long CELL_BYTES = 16;

  /** What parquet's reader builds for each column chunk when it converts the footer. */
  private static final long CHUNK_BYTES = 256;

  static ParquetFooter check(RandomAccessInput in, long budget) throws IOException {
    long length = in.length();
    if (length < 2L * MAGIC.length + 4) {
      throw new MalformedParquetException(
          "not a parquet file: " + length + " bytes is too short for the magic and footer");
    }
    if (!Arrays.equals(bytes(in, 0, MAGIC.length), MAGIC)) {
      throw new MalformedParquetException("not a parquet file: it does not start with PAR1");
    }
    byte[] tail = bytes(in, length - 8, 8);
    if (!Arrays.equals(Arrays.copyOfRange(tail, 4, 8), MAGIC)) {
      throw new MalformedParquetException(
          "not a parquet file, or a cut-off one: it does not end with PAR1");
    }
    long metadataLength =
        Integer.toUnsignedLong(ByteBuffer.wrap(tail, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
    long metadataStart = length - 8 - metadataLength;
    if (metadataStart < MAGIC.length) {
      throw new MalformedParquetException(
          "parquet footer of " + metadataLength + " bytes does not fit in a file of " + length);
    }
    if (metadataLength > budget || metadataLength > Integer.MAX_VALUE - 8) {
      throw new TooLargeException("footer", metadataLength, budget);
    }
    BoundedThrift.Parsed<FileMetaData> parsed =
        BoundedThrift.fileMetaData(bytes(in, metadataStart, (int) metadataLength), budget);
    long heap =
        parsed.heap()
            + CHUNK_BYTES * columnChunks(parsed.value())
            + schemaHeap(parsed.value().getSchema());
    if (heap > budget) {
      throw new TooLargeException("footer", heap, budget);
    }
    rejectUnknownCodecs(parsed.value());
    return new ParquetFooter(metadataStart, heap);
  }

  private static long columnChunks(FileMetaData metadata) {
    if (metadata.getRow_groups() == null) {
      return 0;
    }
    return metadata.getRow_groups().stream()
        .mapToLong(group -> group.getColumns() == null ? 0 : group.getColumns().size())
        .sum();
  }

  /** parquet 1.12.3 cannot convert a footer that names LZ4_RAW, so it fails the whole file. */
  private static void rejectUnknownCodecs(FileMetaData metadata) throws MalformedParquetException {
    if (metadata.getRow_groups() == null) {
      return;
    }
    for (RowGroup group : metadata.getRow_groups()) {
      for (ColumnChunk chunk :
          group.getColumns() == null ? List.<ColumnChunk>of() : group.getColumns()) {
        if (chunk.getMeta_data() != null
            && chunk.getMeta_data().getCodec() == CompressionCodec.LZ4_RAW) {
          throw new MalformedParquetException(
              "parquet codec LZ4_RAW is not supported by parquet 1.12.3");
        }
      }
    }
  }

  /**
   * Walks the flattened schema the way parquet does, without recursion: the root and each group say
   * how many children follow. Returns what its leaves will hold in memory.
   */
  private static long schemaHeap(List<SchemaElement> elements) throws MalformedParquetException {
    if (elements == null
        || elements.isEmpty()
        || !elements.get(0).isSetNum_children()
        || elements.get(0).getNum_children() < 0) {
      throw new MalformedParquetException("parquet schema has no root group");
    }
    int[] childrenLeft = new int[MAX_SCHEMA_DEPTH + 1];
    int[] levels = new int[MAX_SCHEMA_DEPTH + 1];
    int[] repeats = new int[MAX_SCHEMA_DEPTH + 1];
    int depth = 0;
    childrenLeft[0] = elements.get(0).getNum_children();
    long cells = 0;
    int leaves = 0;
    for (int i = 1; i < elements.size(); i++) {
      while (depth > 0 && childrenLeft[depth] == 0) {
        depth--;
      }
      if (childrenLeft[depth] == 0) {
        throw new MalformedParquetException(
            "parquet schema has " + (elements.size() - i) + " elements outside its root");
      }
      childrenLeft[depth]--;
      SchemaElement element = elements.get(i);
      int definition = levels[depth];
      int repetition = repeats[depth];
      if (element.isSetRepetition_type()) {
        switch (element.getRepetition_type()) {
          case OPTIONAL -> definition++;
          case REPEATED -> {
            definition++;
            repetition++;
          }
          default -> {}
        }
      }
      if (element.isSetNum_children()) {
        if (element.getNum_children() < 0) {
          throw new MalformedParquetException("parquet schema group has a negative child count");
        }
        if (depth + 1 > MAX_SCHEMA_DEPTH) {
          throw new MalformedParquetException(
              "parquet schema nests deeper than " + MAX_SCHEMA_DEPTH + " levels");
        }
        depth++;
        childrenLeft[depth] = element.getNum_children();
        levels[depth] = definition;
        repeats[depth] = repetition;
      } else {
        leaves++;
        cells += (long) (depth + 1) * (definition + 1) * (repetition + 1);
        if (cells > MAX_LEVEL_CELLS) {
          throw new MalformedParquetException("parquet schema has too many deeply nested columns");
        }
      }
    }
    while (depth > 0 && childrenLeft[depth] == 0) {
      depth--;
    }
    if (childrenLeft[depth] != 0) {
      throw new MalformedParquetException("parquet schema ends inside a group");
    }
    if (leaves == 0) {
      throw new MalformedParquetException("parquet schema has no columns");
    }
    return LEAF_BYTES * leaves + CELL_BYTES * cells;
  }

  static byte[] bytes(RandomAccessInput in, long offset, int length) throws IOException {
    byte[] bytes = new byte[length];
    try (InputStream range = in.read(offset, length)) {
      range.readNBytes(bytes, 0, length);
    }
    return bytes;
  }
}
