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
package io.fleak.zephflow.lib.serdes.encode;

import static io.fleak.zephflow.lib.utils.CompressionUtils.gzip;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

/** One file per batch, gzipped as a whole when {@code gzip} is set. */
@RequiredArgsConstructor
final class TextFileWriter implements BlobFileWriter<RecordFleakData> {

  private final UnitWriter writer;
  private final String formatExtension;
  private final boolean gzip;

  @Override
  public List<File> writeToTempFiles(List<RecordFleakData> records, Path tempDir) throws Exception {
    byte[] bytes = writer.write(records);
    Path file = tempDir.resolve(UUID.randomUUID() + "." + getFileExtension());
    Files.write(file, gzip ? gzip(bytes) : bytes);
    return List.of(file.toFile());
  }

  @Override
  public String getFileExtension() {
    return gzip ? formatExtension + ".gz" : formatExtension;
  }

  @Override
  public void validateRecord(RecordFleakData record) throws Exception {
    if (record == null) {
      throw new IllegalArgumentException("Record cannot be null");
    }
    writer.write(List.of(record));
  }
}
