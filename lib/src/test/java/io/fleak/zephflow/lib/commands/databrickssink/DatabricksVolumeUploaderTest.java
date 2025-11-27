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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.files.DirectoryEntry;
import com.databricks.sdk.service.files.FilesAPI;
import com.databricks.sdk.service.files.UploadRequest;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class DatabricksVolumeUploaderTest {

  @TempDir Path tempDir;

  private FilesAPI filesAPI;
  private DatabricksVolumeUploader uploader;

  @BeforeEach
  void setUp() {
    WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
    filesAPI = mock(FilesAPI.class);
    when(workspaceClient.files()).thenReturn(filesAPI);
    uploader = new DatabricksVolumeUploader(workspaceClient);
  }

  @Test
  void testUploadFile_success() throws Exception {
    File testFile = createTestFile("test.parquet", "test content");
    String remotePath = "/Volumes/catalog/schema/volume/test.parquet";

    uploader.uploadFile(testFile, remotePath);

    ArgumentCaptor<UploadRequest> captor = ArgumentCaptor.forClass(UploadRequest.class);
    verify(filesAPI).upload(captor.capture());

    UploadRequest request = captor.getValue();
    assertEquals(remotePath, request.getFilePath());
    assertTrue(request.getOverwrite());
    assertNotNull(request.getContents());
  }

  @Test
  void testUploadFile_setsOverwriteTrue() throws Exception {
    File testFile = createTestFile("overwrite-test.parquet", "content");
    String remotePath = "/Volumes/catalog/schema/volume/overwrite-test.parquet";

    uploader.uploadFile(testFile, remotePath);

    ArgumentCaptor<UploadRequest> captor = ArgumentCaptor.forClass(UploadRequest.class);
    verify(filesAPI).upload(captor.capture());
    assertTrue(captor.getValue().getOverwrite(), "Overwrite should be set to true");
  }

  @Test
  void testUploadFile_throwsIOExceptionOnFailure() throws Exception {
    File testFile = createTestFile("fail.parquet", "content");
    String remotePath = "/Volumes/catalog/schema/volume/fail.parquet";

    doThrow(new RuntimeException("Upload failed")).when(filesAPI).upload(any(UploadRequest.class));

    IOException exception =
        assertThrows(IOException.class, () -> uploader.uploadFile(testFile, remotePath));
    assertTrue(exception.getMessage().contains("Upload failed"));
  }

  @Test
  void testUploadFile_wrapsRuntimeExceptionInIOException() throws Exception {
    File testFile = createTestFile("io-fail.parquet", "content");
    String remotePath = "/Volumes/catalog/schema/volume/io-fail.parquet";

    RuntimeException cause = new RuntimeException("Network error");
    doThrow(cause).when(filesAPI).upload(any(UploadRequest.class));

    IOException exception =
        assertThrows(IOException.class, () -> uploader.uploadFile(testFile, remotePath));
    assertTrue(exception.getMessage().contains("Network error"));
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testUploadFile_fileNotFound() {
    File nonExistentFile = new File(tempDir.toFile(), "nonexistent.parquet");
    String remotePath = "/Volumes/catalog/schema/volume/nonexistent.parquet";

    assertThrows(IOException.class, () -> uploader.uploadFile(nonExistentFile, remotePath));
    verify(filesAPI, never()).upload(any());
  }

  @Test
  void testDeleteDirectory_emptyDirectory() {
    String directoryPath = "/Volumes/catalog/schema/volume/batch-123";
    when(filesAPI.listDirectoryContents(directoryPath)).thenReturn(List.of());

    uploader.deleteDirectory(directoryPath);

    verify(filesAPI).listDirectoryContents(directoryPath);
    verify(filesAPI).deleteDirectory(directoryPath);
  }

  @Test
  void testDeleteDirectory_withFiles() {
    String directoryPath = "/Volumes/catalog/schema/volume/batch-123";
    DirectoryEntry file1 =
        createFileEntry("/Volumes/catalog/schema/volume/batch-123/file1.parquet");
    DirectoryEntry file2 =
        createFileEntry("/Volumes/catalog/schema/volume/batch-123/file2.parquet");

    when(filesAPI.listDirectoryContents(directoryPath)).thenReturn(List.of(file1, file2));

    uploader.deleteDirectory(directoryPath);

    verify(filesAPI).delete("/Volumes/catalog/schema/volume/batch-123/file1.parquet");
    verify(filesAPI).delete("/Volumes/catalog/schema/volume/batch-123/file2.parquet");
    verify(filesAPI).deleteDirectory(directoryPath);
  }

  @Test
  void testDeleteDirectory_withNestedDirectory() {
    String parentDir = "/Volumes/catalog/schema/volume/batch-123";
    String childDir = "/Volumes/catalog/schema/volume/batch-123/subdir";

    DirectoryEntry subdir = createDirectoryEntry(childDir);
    DirectoryEntry fileInSubdir = createFileEntry(childDir + "/nested.parquet");

    when(filesAPI.listDirectoryContents(parentDir)).thenReturn(List.of(subdir));
    when(filesAPI.listDirectoryContents(childDir)).thenReturn(List.of(fileInSubdir));

    uploader.deleteDirectory(parentDir);

    verify(filesAPI).delete(childDir + "/nested.parquet");
    verify(filesAPI).deleteDirectory(childDir);
    verify(filesAPI).deleteDirectory(parentDir);
  }

  @Test
  void testDeleteDirectory_handlesExceptionGracefully() {
    String directoryPath = "/Volumes/catalog/schema/volume/batch-123";
    when(filesAPI.listDirectoryContents(directoryPath))
        .thenThrow(new RuntimeException("Directory not found"));

    // Should not throw - exceptions are caught and logged
    assertDoesNotThrow(() -> uploader.deleteDirectory(directoryPath));
  }

  @Test
  void testDeleteDirectory_handlesDeleteFailureGracefully() {
    String directoryPath = "/Volumes/catalog/schema/volume/batch-123";
    when(filesAPI.listDirectoryContents(directoryPath)).thenReturn(List.of());
    doThrow(new RuntimeException("Permission denied"))
        .when(filesAPI)
        .deleteDirectory(directoryPath);

    // Should not throw - exceptions are caught and logged
    assertDoesNotThrow(() -> uploader.deleteDirectory(directoryPath));
  }

  private File createTestFile(String name, String content) throws IOException {
    Path filePath = tempDir.resolve(name);
    Files.writeString(filePath, content);
    return filePath.toFile();
  }

  private DirectoryEntry createFileEntry(String path) {
    DirectoryEntry entry = mock(DirectoryEntry.class);
    when(entry.getPath()).thenReturn(path);
    when(entry.getIsDirectory()).thenReturn(false);
    return entry;
  }

  private DirectoryEntry createDirectoryEntry(String path) {
    DirectoryEntry entry = mock(DirectoryEntry.class);
    when(entry.getPath()).thenReturn(path);
    when(entry.getIsDirectory()).thenReturn(true);
    return entry;
  }
}
