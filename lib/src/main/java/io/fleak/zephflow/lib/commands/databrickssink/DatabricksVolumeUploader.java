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

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.files.DirectoryEntry;
import com.databricks.sdk.service.files.UploadRequest;
import java.io.*;
import java.nio.file.Files;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record DatabricksVolumeUploader(WorkspaceClient workspaceClient, boolean bounded) {
  public DatabricksVolumeUploader(WorkspaceClient workspaceClient) {
    this(workspaceClient, false);
  }

  public void uploadFile(File file, String remotePath) throws IOException {
    if (!bounded)
      log.info("Uploading {} ({} bytes) to {}", file.getName(), file.length(), remotePath);

    try (InputStream inputStream = Files.newInputStream(file.toPath())) {
      UploadRequest request =
          new UploadRequest().setFilePath(remotePath).setContents(inputStream).setOverwrite(true);
      workspaceClient.files().upload(request);
      if (!bounded) log.info("Upload completed for {}", file.getName());
    } catch (Exception e) {
      if (bounded) log.error("Bounded Databricks upload failed");
      else
        log.error(
            "Upload failed for {} to {}: {} - {}",
            file.getName(),
            remotePath,
            e.getClass().getName(),
            e.getMessage(),
            e);
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException("Upload failed: " + e.getMessage(), e);
    }
  }

  public void deleteDirectory(String directoryPath) {
    if (!bounded) log.debug("Deleting directory: {}", directoryPath);
    try {
      // Delete all files in the directory first
      for (DirectoryEntry entry : workspaceClient.files().listDirectoryContents(directoryPath)) {
        if (entry.getIsDirectory()) {
          deleteDirectory(entry.getPath());
        } else {
          workspaceClient.files().delete(entry.getPath());
          if (!bounded) log.debug("Deleted file: {}", entry.getPath());
        }
      }
      workspaceClient.files().deleteDirectory(directoryPath);
      if (!bounded) log.info("Deleted directory: {}", directoryPath);
    } catch (Exception e) {
      if (bounded) log.warn("Bounded Databricks remote cleanup failed");
      else log.warn("Failed to delete directory {}: {}", directoryPath, e.getMessage());
    }
  }
}
