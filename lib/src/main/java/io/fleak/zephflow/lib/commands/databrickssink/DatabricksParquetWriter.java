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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
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
}
