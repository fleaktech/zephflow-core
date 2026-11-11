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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSourceConfigValidatorTest {

  private final FileSourceConfigValidator validator = new FileSourceConfigValidator();

  @Test
  void missingDecodingIsRejected() throws IOException {
    FileSourceDto.Config config = FileSourceDto.Config.builder().filePath(existingFile()).build();

    assertEquals("decoding is required", decodingRejection(config));
  }

  @Test
  void decodingIsAccepted() throws IOException {
    FileSourceDto.Config config =
        FileSourceDto.Config.builder()
            .filePath(existingFile())
            .decoding(
                new DecodingConfig(
                    List.of(DecompressionCodec.ZSTD),
                    new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void aDecodingTheCompilerRejectsFailsValidation() throws IOException {
    FileSourceDto.Config config =
        FileSourceDto.Config.builder()
            .filePath(existingFile())
            .decoding(
                new DecodingConfig(List.of(DecompressionCodec.GZIP), new ParquetFormatConfig()))
            .build();

    assertEquals(
        "parquet cannot be combined with compression; its pages carry their own",
        decodingRejection(config));
  }

  private String decodingRejection(FileSourceDto.Config config) {
    return assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null))
        .getMessage();
  }

  @TempDir File decodingDir;

  private String existingFile() throws IOException {
    File file = new File(decodingDir, "input.txt");
    file.createNewFile();
    return file.getAbsolutePath();
  }
}
