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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.schema.MessageType;

/**
 * The file's footer, read before anything else: the trailing length and magic, then the metadata,
 * which may be up to {@code maxDocumentBytes}. The schema is checked before parquet builds it, so
 * that its depth and the record reader it implies stay small.
 */
record ParquetFooter(MessageType schema, List<RowGroup> rowGroups, long metadataStart) {

  private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
  private static final int MAX_SCHEMA_DEPTH = 32;

  /** Bounds parquet's record reader tables, which grow with each leaf's depth and levels. */
  private static final long MAX_LEVEL_CELLS = 4_000_000;

  static ParquetFooter read(RandomAccessInput in, DecodeLimits limits) throws IOException {
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
    if (limits.maxDocumentBytes() > 0 && metadataLength > limits.maxDocumentBytes()) {
      throw new FooterTooLargeException(metadataLength);
    }
    FileMetaData metadata =
        BoundedThrift.fileMetaData(bytes(in, metadataStart, (int) metadataLength));
    checkSchema(metadata.getSchema());
    List<RowGroup> rowGroups = metadata.getRow_groups();
    metadata.setRow_groups(new ArrayList<>());
    MessageType schema;
    try {
      schema =
          new ParquetMetadataConverter()
              .fromParquetMetadata(metadata)
              .getFileMetaData()
              .getSchema();
    } catch (RuntimeException e) {
      throw new MalformedParquetException("parquet schema is invalid: " + e.getMessage(), e);
    }
    return new ParquetFooter(schema, rowGroups == null ? List.of() : rowGroups, metadataStart);
  }

  /** A footer over {@code maxDocumentBytes}; it is never read. */
  static final class FooterTooLargeException extends IOException {
    final long size;

    FooterTooLargeException(long size) {
      super("parquet footer of " + size + " bytes is over maxDocumentBytes");
      this.size = size;
    }
  }

  /**
   * Walks the flattened schema the way parquet does, without recursion: the root and each group say
   * how many children follow.
   */
  private static void checkSchema(List<SchemaElement> elements) throws MalformedParquetException {
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
  }

  static byte[] bytes(RandomAccessInput in, long offset, int length) throws IOException {
    byte[] bytes = new byte[length];
    try (InputStream range = in.read(offset, length)) {
      int read = range.readNBytes(bytes, 0, length);
      if (read != length) {
        throw new IOException("the input range ended " + (length - read) + " bytes early");
      }
    }
    return bytes;
  }
}
