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
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointStore;
import java.io.IOException;

public final class FsSourceExecutionContext implements ExecutionContext {

  FsBackend backend;
  FsBackendConfig backendConfig;
  FileLister lister;
  FileReader reader;
  CheckpointStore checkpointStore;

  @Override
  public void close() throws IOException {
    if (lister != null)
      try {
        lister.close();
      } catch (Exception ignored) {
      }
    if (reader != null)
      try {
        reader.close();
      } catch (Exception ignored) {
      }
    if (checkpointStore != null)
      try {
        checkpointStore.close();
      } catch (Exception ignored) {
      }
  }
}
