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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class TextS3FileWriterTest {

  @TempDir Path tempDir;

  @Mock private FleakSerializer<Object> mockSerializer;

  private static final byte[] TEST_DATA = "test-data".getBytes();

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(mockSerializer.serialize(anyList()))
        .thenReturn(new SerializedEvent(null, TEST_DATA, Map.of()));
  }

  @Test
  void testValidateRecord_nullRecord() {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE);
    assertThrows(IllegalArgumentException.class, () -> writer.validateRecord(null));
  }

  @Test
  void testValidateRecord_validRecord() throws Exception {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE);
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));
    assertDoesNotThrow(() -> writer.validateRecord(record));
    verify(mockSerializer).serialize(List.of(record));
  }

  @Test
  void testValidateRecord_unserializableRecord() throws Exception {
    FleakSerializer<Object> failingSerializer = mock(FleakSerializer.class);
    when(failingSerializer.serialize(anyList()))
        .thenThrow(new RuntimeException("Serialization failed"));

    TextS3FileWriter writer =
        new TextS3FileWriter(failingSerializer, EncodingType.JSON_OBJECT_LINE);
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));

    assertThrows(RuntimeException.class, () -> writer.validateRecord(record));
  }

  @Test
  void testWriteToTempFiles() throws Exception {
    RecordFleakData record1 = (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test1"));
    RecordFleakData record2 = (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "test2"));

    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE);
    List<File> files = writer.writeToTempFiles(List.of(record1, record2), tempDir);

    assertEquals(1, files.size());
    File outputFile = files.get(0);
    assertTrue(outputFile.exists());
    assertTrue(outputFile.getName().endsWith(".jsonl"));
    byte[] content = Files.readAllBytes(outputFile.toPath());
    assertArrayEquals(TEST_DATA, content);
    verify(mockSerializer).serialize(List.of(record1, record2));
  }

  @Test
  void testGetFileExtension_jsonl() {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE);
    assertEquals("jsonl", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_json() {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.JSON_OBJECT);
    assertEquals("json", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_csv() {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.CSV);
    assertEquals("csv", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_txt() {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.TEXT);
    assertEquals("txt", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_xml() {
    TextS3FileWriter writer = new TextS3FileWriter(mockSerializer, EncodingType.XML);
    assertEquals("xml", writer.getFileExtension());
  }
}
