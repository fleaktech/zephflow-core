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
package io.fleak.zephflow.lib.serdes.decode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import shaded.parquet.org.apache.thrift.TBase;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/** Parquet files for tests: written by parquet-mr, or assembled page by page. */
final class ParquetTestFiles {

  private ParquetTestFiles() {}

  /** Writes {@code rows} with parquet-mr's example writer into memory. */
  static byte[] write(
      MessageType schema, List<Group> rows, Consumer<ExampleParquetWriter.Builder> options) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    write(bytes, schema, rows, options);
    return bytes.toByteArray();
  }

  static void write(
      Path file,
      MessageType schema,
      Iterable<Group> rows,
      Consumer<ExampleParquetWriter.Builder> options) {
    try (OutputStream out = Files.newOutputStream(file)) {
      write(out, schema, rows, options);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void write(
      OutputStream out,
      MessageType schema,
      Iterable<Group> rows,
      Consumer<ExampleParquetWriter.Builder> options) {
    ExampleParquetWriter.Builder builder =
        ExampleParquetWriter.builder(new StreamOutputFile(out))
            .withType(schema)
            .withConf(new Configuration(false));
    options.accept(builder);
    try (ParquetWriter<Group> writer = builder.build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final class StreamOutputFile implements OutputFile {
    private final OutputStream out;

    StreamOutputFile(OutputStream out) {
      this.out = out;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new PositionOutputStream() {
        private long pos;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          out.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          out.write(b, off, len);
          pos += len;
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  record RawPage(PageHeader header, byte[] data) {}

  /** One column chunk: {@code numValues} values in {@code pages}, stored with {@code codec}. */
  record RawChunk(
      String column, Type type, CompressionCodec codec, long numValues, List<RawPage> pages) {}

  /**
   * A file of row groups of {@code rows} rows each, whose root holds one column per chunk of the
   * first group, each required or as {@code repetition} says, then passed through {@code footer}.
   */
  static byte[] assemble(
      FieldRepetitionType repetition,
      long rows,
      List<List<RawChunk>> groups,
      UnaryOperator<FileMetaData> footer) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(magic());
    List<SchemaElement> schema = new ArrayList<>();
    schema.add(new SchemaElement("schema").setNum_children(groups.get(0).size()));
    for (RawChunk chunk : groups.get(0)) {
      schema.add(
          new SchemaElement(chunk.column()).setType(chunk.type()).setRepetition_type(repetition));
    }
    List<RowGroup> rowGroups = new ArrayList<>();
    for (List<RawChunk> chunks : groups) {
      List<ColumnChunk> columns = new ArrayList<>();
      long total = 0;
      for (RawChunk chunk : chunks) {
        long offset = out.size();
        long dataOffset = offset;
        for (RawPage page : chunk.pages()) {
          if (page.header().getType() == PageType.DICTIONARY_PAGE) {
            dataOffset = offset + thrift(page.header()).length + page.data().length;
          }
          out.writeBytes(thrift(page.header()));
          out.writeBytes(page.data());
        }
        long size = out.size() - offset;
        total += size;
        ColumnMetaData meta =
            new ColumnMetaData(
                chunk.type(),
                List.of(Encoding.PLAIN, Encoding.RLE),
                List.of(chunk.column()),
                chunk.codec(),
                chunk.numValues(),
                size,
                size,
                dataOffset);
        if (dataOffset != offset) {
          meta.setDictionary_page_offset(offset);
        }
        columns.add(new ColumnChunk(offset).setMeta_data(meta));
      }
      rowGroups.add(new RowGroup(columns, total, rows));
    }
    FileMetaData metadata =
        footer.apply(new FileMetaData(1, schema, rows * groups.size(), rowGroups));
    return withFooter(out.toByteArray(), thrift(metadata));
  }

  /** {@code data} followed by {@code footer}, its little-endian length and the magic. */
  static byte[] withFooter(byte[] data, byte[] footer) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.writeBytes(data);
    out.writeBytes(footer);
    out.writeBytes(
        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.length).array());
    out.writeBytes(magic());
    return out.toByteArray();
  }

  static byte[] thrift(TBase<?, ?> value) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      value.write(new TCompactProtocol(new TIOStreamTransport(out)));
    } catch (TException e) {
      throw new IllegalStateException(e);
    }
    return out.toByteArray();
  }

  static byte[] magic() {
    return "PAR1".getBytes(StandardCharsets.US_ASCII);
  }
}
