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
package io.fleak.zephflow.lib.commands.databrickssink;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeDataConverter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class DatabricksParquetWriter {
  private static final int CHUNK_SIZE = 1000;

  private final Engine engine;
  private final StructType schema;

  public DatabricksParquetWriter(StructType schema) {
    Configuration hadoopConf = new Configuration();
    this.engine = DefaultEngine.create(hadoopConf);
    this.schema = schema;
  }

  public List<File> writeParquetFiles(List<Map<String, Object>> data, Path tempDirectory)
      throws IOException {

    if (data.isEmpty()) {
      return Collections.emptyList();
    }

    try (var columnarBatch = DeltaLakeDataConverter.convertToColumnarBatch(data, schema)) {

      String targetDir = tempDirectory.toAbsolutePath().toString();
      List<Column> statsColumns = Collections.emptyList();

      List<File> generatedFiles = new ArrayList<>();

      try (CloseableIterator<DataFileStatus> fileStatusIter =
          engine.getParquetHandler().writeParquetFiles(targetDir, columnarBatch, statsColumns)) {

        while (fileStatusIter.hasNext()) {
          DataFileStatus fileStatus = fileStatusIter.next();
          String path = fileStatus.getPath();
          // Delta Kernel returns paths in file:// URI format, convert to filesystem path
          if (path.startsWith("file:")) {
            path = URI.create(path).getPath();
          }
          File file = new File(path);
          generatedFiles.add(file);
          log.debug("Generated Parquet file: {} ({} bytes)", file.getName(), fileStatus.getSize());
        }
      }

      log.info("Generated {} Parquet files in {}", generatedFiles.size(), targetDir);
      return generatedFiles;
    }
  }

  /**
   * Write Parquet files from RecordFleakData records using chunked processing. Only one chunk
   * (CHUNK_SIZE records) is held in memory at a time to reduce memory pressure.
   */
  public List<File> writeParquetFilesChunked(List<RecordFleakData> records, Path tempDirectory)
      throws IOException {
    if (records.isEmpty()) {
      return Collections.emptyList();
    }

    try (var chunkedBatch = createChunkedIterator(records)) {
      String targetDir = tempDirectory.toAbsolutePath().toString();
      List<Column> statsColumns = Collections.emptyList();

      List<File> generatedFiles = new ArrayList<>();

      try (CloseableIterator<DataFileStatus> fileStatusIter =
          engine.getParquetHandler().writeParquetFiles(targetDir, chunkedBatch, statsColumns)) {

        while (fileStatusIter.hasNext()) {
          DataFileStatus fileStatus = fileStatusIter.next();
          String path = fileStatus.getPath();
          if (path.startsWith("file:")) {
            path = URI.create(path).getPath();
          }
          File file = new File(path);
          generatedFiles.add(file);
          log.debug("Generated Parquet file: {} ({} bytes)", file.getName(), fileStatus.getSize());
        }
      }

      log.info("Generated {} Parquet files in {}", generatedFiles.size(), targetDir);
      return generatedFiles;
    }
  }

  private CloseableIterator<FilteredColumnarBatch> createChunkedIterator(
      List<RecordFleakData> records) {
    return new CloseableIterator<>() {
      private int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < records.size();
      }

      @Override
      public FilteredColumnarBatch next() {
        int end = Math.min(idx + CHUNK_SIZE, records.size());
        List<Map<String, Object>> chunk = new ArrayList<>(end - idx);
        for (int i = idx; i < end; i++) {
          chunk.add(records.get(i).unwrap());
        }
        idx = end;
        return DeltaLakeDataConverter.convertSingleBatch(chunk, schema);
      }

      @Override
      public void close() {}
    };
  }
}
