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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.StructType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/**
 * Writes columnar batches to uncompressed Parquet files without Hadoop on the classpath. A new file
 * starts once the current one reaches 128MB, as delta-kernel's default writer did.
 */
public final class ColumnarBatchParquetWriter {

  static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;

  private final StructType schema;
  private final MessageType parquetSchema;
  private final long targetFileSize;

  public ColumnarBatchParquetWriter(StructType schema) {
    this(schema, TARGET_FILE_SIZE);
  }

  ColumnarBatchParquetWriter(StructType schema, long targetFileSize) {
    this.schema = schema;
    this.parquetSchema = ParquetSchemas.toParquetSchema(schema);
    this.targetFileSize = targetFileSize;
  }

  /** Writes every selected row into new files under {@code directory}; returns them in order. */
  public List<File> write(Iterator<FilteredColumnarBatch> batches, Path directory)
      throws IOException {
    List<File> files = new ArrayList<>();
    BatchWriteSupport support = new BatchWriteSupport(parquetSchema);
    ParquetWriter<Integer> writer = null;
    try {
      while (batches.hasNext()) {
        FilteredColumnarBatch filtered = batches.next();
        ColumnarBatch batch = filtered.getData();
        if (!schema.equals(batch.getSchema())) {
          throw new IllegalArgumentException(
              "Batch schema " + batch.getSchema() + " does not match " + schema);
        }
        support.columns = ParquetColumnWriters.forBatch(batch);
        Optional<ColumnVector> selection = filtered.getSelectionVector();
        for (int row = 0; row < batch.getSize(); row++) {
          if (selection.isPresent()
              && (selection.get().isNullAt(row) || !selection.get().getBoolean(row))) {
            continue;
          }
          if (writer == null) {
            // Hadoop's FileSystem.create made missing parents; LocalOutputFile does not.
            Files.createDirectories(directory);
            File file = directory.resolve(nextFileName(files.size())).toFile();
            files.add(file);
            writer = new Builder(new LocalOutputFile(file.toPath()), support).build();
          }
          writer.write(row);
          if (writer.getDataSize() >= targetFileSize) {
            ParquetWriter<Integer> full = writer;
            writer = null;
            full.close();
          }
        }
      }
    } catch (IOException | RuntimeException e) {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException | RuntimeException closeFailure) {
          e.addSuppressed(closeFailure);
        }
      }
      throw e;
    }
    if (writer != null) {
      writer.close();
    }
    return files;
  }

  private static String nextFileName(int index) {
    return String.format("%s-%03d.parquet", UUID.randomUUID(), index);
  }

  private static final class Builder extends ParquetWriter.Builder<Integer, Builder> {
    private final WriteSupport<Integer> support;

    Builder(OutputFile file, WriteSupport<Integer> support) {
      super(file);
      this.support = support;
      withConf(new PlainParquetConfiguration());
      withCodecFactory(UncompressedCodecs.INSTANCE);
      withCompressionCodec(CompressionCodecName.UNCOMPRESSED);
      // delta-kernel's parquet 1.12 wrote no size statistics.
      withSizeStatisticsEnabled(false);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<Integer> getWriteSupport(Configuration conf) {
      return support;
    }

    @Override
    protected WriteSupport<Integer> getWriteSupport(ParquetConfiguration conf) {
      return support;
    }
  }

  /** Rows are batch row ids; the column writers of the current batch read the values. */
  private static final class BatchWriteSupport extends WriteSupport<Integer> {
    private final MessageType schema;
    private ParquetColumnWriters.ColumnWriter[] columns;
    private RecordConsumer consumer;

    BatchWriteSupport(MessageType schema) {
      this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
      return new WriteContext(schema, Map.of());
    }

    @Override
    public WriteContext init(ParquetConfiguration configuration) {
      return new WriteContext(schema, Map.of());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.consumer = recordConsumer;
    }

    @Override
    public void write(Integer rowId) {
      consumer.startMessage();
      for (ParquetColumnWriters.ColumnWriter column : columns) {
        column.write(consumer, rowId);
      }
      consumer.endMessage();
    }
  }

  /** Parquet's own codec factory loads Hadoop even for uncompressed pages. */
  private enum UncompressedCodecs implements CompressionCodecFactory {
    INSTANCE;

    private static final BytesInputCompressor PASS_THROUGH =
        new BytesInputCompressor() {
          @Override
          public BytesInput compress(BytesInput bytes) {
            return bytes;
          }

          @Override
          public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
          }

          @Override
          public void release() {}
        };

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codec) {
      if (codec != CompressionCodecName.UNCOMPRESSED) {
        throw new UnsupportedOperationException("only uncompressed Parquet is written: " + codec);
      }
      return PASS_THROUGH;
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codec) {
      throw new UnsupportedOperationException("the writer does not decompress");
    }

    @Override
    public void release() {}
  }
}
