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
package io.fleak.zephflow.lib.commands.sink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetBlobFileWriterTest {

  @TempDir Path tempDir;

  private static final Map<String, Object> TEST_AVRO_SCHEMA =
      Map.of(
          "type", "record",
          "name", "TestRecord",
          "fields",
              List.of(
                  Map.of("name", "id", "type", "int"),
                  Map.of("name", "name", "type", "string"),
                  Map.of("name", "value", "type", "double")));

  private ParquetBlobFileWriter writer;

  @BeforeEach
  void setUp() {
    writer = new ParquetBlobFileWriter(TEST_AVRO_SCHEMA);
  }

  @Test
  void testGetFileExtension() {
    assertEquals("parquet", writer.getFileExtension());
  }

  @Test
  void testValidateRecord_nullRecord() {
    assertThrows(IllegalArgumentException.class, () -> writer.validateRecord(null));
  }

  @Test
  void testValidateRecord_validRecord() {
    Map<String, Object> data = Map.of("id", 1, "name", "test", "value", 42.0);
    RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
    assertDoesNotThrow(() -> writer.validateRecord(record));
  }

  @Test
  void testWriteToTempFiles() throws Exception {
    Map<String, Object> data1 = new HashMap<>();
    data1.put("id", 1);
    data1.put("name", "test1");
    data1.put("value", 10.5);

    Map<String, Object> data2 = new HashMap<>();
    data2.put("id", 2);
    data2.put("name", "test2");
    data2.put("value", 20.5);

    RecordFleakData record1 = (RecordFleakData) FleakData.wrap(data1);
    RecordFleakData record2 = (RecordFleakData) FleakData.wrap(data2);

    List<File> parquetFiles = writer.writeToTempFiles(List.of(record1, record2), tempDir);

    assertFalse(parquetFiles.isEmpty());
    for (File parquetFile : parquetFiles) {
      assertTrue(parquetFile.exists());
      assertTrue(parquetFile.getName().endsWith(".parquet"));
      assertTrue(parquetFile.length() > 0);
    }
  }

  @Test
  void testWriteToTempFiles_singleRecord() throws Exception {
    Map<String, Object> data = new HashMap<>();
    data.put("id", 1);
    data.put("name", "single");
    data.put("value", 42.0);

    RecordFleakData record = (RecordFleakData) FleakData.wrap(data);

    List<File> parquetFiles = writer.writeToTempFiles(List.of(record), tempDir);

    assertFalse(parquetFiles.isEmpty());
    for (File parquetFile : parquetFiles) {
      assertTrue(parquetFile.exists());
      assertTrue(parquetFile.length() > 0);
    }
  }
}
