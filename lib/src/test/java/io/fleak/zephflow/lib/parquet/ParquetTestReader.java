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
package io.fleak.zephflow.lib.parquet;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.parquet.HadoopFreeReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/** Reads uncompressed Parquet files into example {@link Group}s without Hadoop. */
public final class ParquetTestReader {

  private ParquetTestReader() {}

  public static List<Group> readGroups(List<File> files) throws IOException {
    List<Group> rows = new ArrayList<>();
    for (File file : files) {
      try (ParquetFileReader reader = open(file)) {
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        MessageColumnIO columns = new ColumnIOFactory().getColumnIO(schema);
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
          RecordReader<Group> records =
              columns.getRecordReader(pages, new GroupRecordConverter(schema));
          for (long i = 0; i < pages.getRowCount(); i++) {
            rows.add(records.read());
          }
        }
      }
    }
    return rows;
  }

  /** One file's rows, read one at a time like parquet's {@code ParquetReader}. */
  public static Rows rows(File file) throws IOException {
    return new Rows(readGroups(List.of(file)).iterator());
  }

  public static final class Rows {
    private final Iterator<Group> rows;

    private Rows(Iterator<Group> rows) {
      this.rows = rows;
    }

    /** The next row, or null after the last. */
    public Group read() {
      return rows.hasNext() ? rows.next() : null;
    }
  }

  public static ParquetMetadata footer(File file) throws IOException {
    try (ParquetFileReader reader = open(file)) {
      return reader.getFooter();
    }
  }

  private static ParquetFileReader open(File file) throws IOException {
    return new ParquetFileReader(
        new LocalInputFile(file.toPath()), HadoopFreeReadOptions.create(Uncompressed.INSTANCE));
  }

  private enum Uncompressed implements CompressionCodecFactory {
    INSTANCE;

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codec) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codec) {
      if (codec != CompressionCodecName.UNCOMPRESSED) {
        throw new UnsupportedOperationException("test reader only reads uncompressed: " + codec);
      }
      return new BytesInputDecompressor() {
        @Override
        public BytesInput decompress(BytesInput bytes, int size) {
          return bytes;
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int size) {
          byte[] copy = new byte[compressedSize];
          input.get(copy);
          output.put(copy);
        }

        @Override
        public void release() {}
      };
    }

    @Override
    public void release() {}
  }
}
