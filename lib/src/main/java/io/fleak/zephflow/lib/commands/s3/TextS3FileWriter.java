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

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * S3FileWriter implementation for text-based formats (CSV, JSON, JSONL, TEXT, XML). Uses
 * FleakSerializer to serialize records to byte arrays.
 */
public class TextS3FileWriter implements S3FileWriter<RecordFleakData> {

  private final FleakSerializer<?> serializer;
  private final EncodingType encodingType;

  public TextS3FileWriter(FleakSerializer<?> serializer, EncodingType encodingType) {
    this.serializer = serializer;
    this.encodingType = encodingType;
  }

  @Override
  public List<File> writeToTempFiles(List<RecordFleakData> records, Path tempDir) throws Exception {
    SerializedEvent serializedEvent = serializer.serialize(records);
    byte[] data = serializedEvent.value();

    String filename = UUID.randomUUID() + "." + getFileExtension();
    Path tempFile = tempDir.resolve(filename);
    Files.write(tempFile, data);
    return List.of(tempFile.toFile());
  }

  @Override
  public String getFileExtension() {
    return encodingType.getFileExtension();
  }

  @Override
  public void validateRecord(RecordFleakData record) throws Exception {
    if (record == null) {
      throw new IllegalArgumentException("Record cannot be null");
    }
  }
}
