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
package io.fleak.zephflow.lib.commands.s3;

import io.delta.kernel.types.StructType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.AvroToDeltaSchemaConverter;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksParquetWriter;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeDataConverter;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * S3FileWriter implementation for Parquet format. Uses Delta Kernel to write Parquet files with
 * specified Avro schema.
 */
@Slf4j
public class ParquetS3FileWriter implements S3FileWriter<RecordFleakData> {

  private final StructType schema;
  private final DatabricksParquetWriter parquetWriter;

  public ParquetS3FileWriter(Map<String, Object> avroSchema) {
    this.schema = AvroToDeltaSchemaConverter.parse(avroSchema);
    this.parquetWriter = new DatabricksParquetWriter(schema);
  }

  @Override
  public List<File> writeToTempFiles(List<RecordFleakData> records, Path tempDir) throws Exception {
    List<File> parquetFiles = parquetWriter.writeParquetFilesChunked(records, tempDir);

    if (parquetFiles.isEmpty()) {
      throw new IllegalStateException("No parquet files were generated");
    }

    log.info("Generated {} Parquet file(s)", parquetFiles.size());
    return parquetFiles;
  }

  @Override
  public String getFileExtension() {
    return "parquet";
  }

  @Override
  public void validateRecord(RecordFleakData record) throws Exception {
    if (record == null) {
      throw new IllegalArgumentException("Record cannot be null");
    }
    try (var batch =
        DeltaLakeDataConverter.convertToColumnarBatch(List.of(record.unwrap()), schema)) {
      while (batch.hasNext()) {
        batch.next();
      }
    }
  }
}
