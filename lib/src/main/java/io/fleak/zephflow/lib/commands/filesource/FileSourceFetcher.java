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
package io.fleak.zephflow.lib.commands.filesource;

import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;

/** Created by bolei on 3/24/25 */
public class FileSourceFetcher implements Fetcher<SerializedEvent> {

  private final File file;

  public FileSourceFetcher(File file) {
    this.file = file;
  }

  @Override
  public List<SerializedEvent> fetch() {
    byte[] bytes;
    try {
      bytes = FileUtils.readFileToByteArray(file);
    } catch (IOException e) {
      throw new IllegalStateException(
          "failed to read data from given file " + file.getAbsolutePath());
    }
    SerializedEvent serializedEvent = new SerializedEvent(null, bytes, null);
    return List.of(serializedEvent);
  }

  @Override
  public void close() throws IOException {}
}
