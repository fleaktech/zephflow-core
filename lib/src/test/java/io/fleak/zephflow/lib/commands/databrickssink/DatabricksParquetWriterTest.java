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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabricksParquetWriterTest {

  private static final StructType TEST_SCHEMA =
      new StructType()
          .add(new StructField("id", IntegerType.INTEGER, false))
          .add(new StructField("name", StringType.STRING, false));

  private Path tempDir;
  private DatabricksParquetWriter writer;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("parquet-writer-test");
    writer = new DatabricksParquetWriter(TEST_SCHEMA);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tempDir != null && Files.exists(tempDir)) {
      try (var paths = Files.walk(tempDir)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  @Test
  void testWriteParquetFiles_generatesValidFiles() throws Exception {
    List<Map<String, Object>> data =
        List.of(Map.of("id", 1, "name", "Alice"), Map.of("id", 2, "name", "Bob"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File parquetFile = files.get(0);
    assertTrue(parquetFile.exists(), "Parquet file should exist");
    assertTrue(parquetFile.length() > 0, "Parquet file should have content");
    assertTrue(parquetFile.getName().endsWith(".parquet"), "File should have .parquet extension");
  }

  @Test
  void testWriteParquetFiles_returnsFilesystemPathsNotUriPaths() throws Exception {
    List<Map<String, Object>> data = List.of(Map.of("id", 1, "name", "Test"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File file = files.get(0);

    // The bug was that Delta Kernel returns file:// URI paths, and File was created with that URI
    // which caused file.exists() to return false and file operations to fail
    assertFalse(
        file.getPath().startsWith("file:"), "File path should not start with 'file:' URI scheme");
    assertTrue(file.exists(), "File should exist at the returned path");
    assertTrue(file.canRead(), "File should be readable");
  }

  @Test
  void testWriteParquetFiles_filePathIsAbsolute() throws Exception {
    List<Map<String, Object>> data = List.of(Map.of("id", 1, "name", "Test"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File file = files.get(0);
    assertTrue(file.isAbsolute(), "Returned file path should be absolute");
    assertTrue(
        file.getAbsolutePath().contains(tempDir.toAbsolutePath().toString()),
        "File should be in temp directory");
  }

  @Test
  void testWriteParquetFiles_emptyDataReturnsEmptyList() throws Exception {
    List<Map<String, Object>> data = List.of();

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertTrue(files.isEmpty(), "Empty data should return empty file list");
  }

  @Test
  void testWriteParquetFiles_multipleRecords() throws Exception {
    List<Map<String, Object>> data =
        List.of(
            Map.of("id", 1, "name", "Alice"),
            Map.of("id", 2, "name", "Bob"),
            Map.of("id", 3, "name", "Carol"),
            Map.of("id", 4, "name", "David"),
            Map.of("id", 5, "name", "Eve"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertFalse(files.isEmpty(), "Should generate at least one file");
    for (File file : files) {
      assertTrue(file.exists(), "All returned files should exist");
      assertFalse(file.getPath().startsWith("file:"), "No file path should have URI scheme");
    }
  }

  @Test
  void testWriteParquetFiles_filesCanBeReadAsInputStream() throws Exception {
    List<Map<String, Object>> data = List.of(Map.of("id", 1, "name", "Test"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File file = files.get(0);

    // This is the actual operation that failed before the fix
    // DatabricksVolumeUploader uses Files.newInputStream() to read the file for upload
    try (var inputStream = Files.newInputStream(file.toPath())) {
      byte[] bytes = inputStream.readAllBytes();
      assertTrue(bytes.length > 0, "Should be able to read file contents");
    }
  }
}
