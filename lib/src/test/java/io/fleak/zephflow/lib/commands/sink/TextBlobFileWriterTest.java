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

import static io.fleak.zephflow.lib.utils.CompressionUtils.gunzip;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.CompressorFactory;
import io.fleak.zephflow.lib.serdes.compression.NoopCompressor;
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

class TextBlobFileWriterTest {

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
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE, new NoopCompressor());
    assertThrows(IllegalArgumentException.class, () -> writer.validateRecord(null));
  }

  @Test
  void testValidateRecord_validRecord() throws Exception {
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE, new NoopCompressor());
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));
    assertDoesNotThrow(() -> writer.validateRecord(record));
    verify(mockSerializer).serialize(List.of(record));
  }

  @Test
  void testValidateRecord_unserializableRecord() throws Exception {
    FleakSerializer<Object> failingSerializer = mock(FleakSerializer.class);
    when(failingSerializer.serialize(anyList()))
        .thenThrow(new RuntimeException("Serialization failed"));

    TextBlobFileWriter writer =
        new TextBlobFileWriter(
            failingSerializer, EncodingType.JSON_OBJECT_LINE, new NoopCompressor());
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));

    assertThrows(RuntimeException.class, () -> writer.validateRecord(record));
  }

  @Test
  void testWriteToTempFiles() throws Exception {
    RecordFleakData record1 = (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test1"));
    RecordFleakData record2 = (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "test2"));

    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE, new NoopCompressor());
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
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.JSON_OBJECT_LINE, new NoopCompressor());
    assertEquals("jsonl", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_json() {
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.JSON_OBJECT, new NoopCompressor());
    assertEquals("json", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_csv() {
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.CSV, new NoopCompressor());
    assertEquals("csv", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_txt() {
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.TEXT, new NoopCompressor());
    assertEquals("txt", writer.getFileExtension());
  }

  @Test
  void testGetFileExtension_xml() {
    TextBlobFileWriter writer =
        new TextBlobFileWriter(mockSerializer, EncodingType.XML, new NoopCompressor());
    assertEquals("xml", writer.getFileExtension());
  }

  @Test
  void testWriteToTempFiles_gzip() throws Exception {
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1));

    TextBlobFileWriter writer =
        new TextBlobFileWriter(
            mockSerializer,
            EncodingType.JSON_OBJECT_LINE,
            CompressorFactory.getCompressor(CompressionType.GZIP));
    List<File> files = writer.writeToTempFiles(List.of(record), tempDir);

    assertEquals(1, files.size());
    File outputFile = files.get(0);
    assertTrue(outputFile.getName().endsWith(".jsonl.gz"));
    assertArrayEquals(TEST_DATA, gunzip(Files.readAllBytes(outputFile.toPath())));
  }

  @Test
  void testGetFileExtension_gzip() {
    TextBlobFileWriter writer =
        new TextBlobFileWriter(
            mockSerializer,
            EncodingType.CSV,
            CompressorFactory.getCompressor(CompressionType.GZIP));
    assertEquals("csv.gz", writer.getFileExtension());
  }
}
