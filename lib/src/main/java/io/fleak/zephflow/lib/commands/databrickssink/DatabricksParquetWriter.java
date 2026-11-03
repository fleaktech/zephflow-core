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

import com.google.common.collect.Lists;
import io.delta.kernel.types.StructType;
import io.fleak.zephflow.lib.parquet.ColumnarBatchConverter;
import io.fleak.zephflow.lib.parquet.ColumnarBatchParquetWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksParquetWriter {
  private static final int CHUNK_SIZE = 1000;

  private final StructType schema;
  private final ColumnarBatchParquetWriter writer;

  public DatabricksParquetWriter(StructType schema) {
    this.schema = schema;
    this.writer = new ColumnarBatchParquetWriter(schema);
  }

  public List<File> writeParquetFiles(List<Map<String, Object>> data, Path tempDirectory)
      throws IOException {
    if (data.isEmpty()) {
      return Collections.emptyList();
    }
    Path directory = tempDirectory.toAbsolutePath();
    List<File> files =
        writer.write(
            Lists.partition(data, CHUNK_SIZE).stream()
                .map(chunk -> ColumnarBatchConverter.convertSingleBatch(chunk, schema))
                .iterator(),
            directory);
    log.info("Generated {} Parquet files in {}", files.size(), directory);
    return files;
  }
}
