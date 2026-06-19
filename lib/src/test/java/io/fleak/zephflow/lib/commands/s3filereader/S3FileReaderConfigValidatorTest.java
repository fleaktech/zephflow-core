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
package io.fleak.zephflow.lib.commands.s3filereader;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class S3FileReaderConfigValidatorTest {

  private final S3FileReaderConfigValidator validator = new S3FileReaderConfigValidator();

  @Test
  void acceptsValidLineConfig() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder().pathField("s3Path").region("us-east-1").build();
    assertDoesNotThrow(() -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }

  @Test
  void rejectsBlankPathField() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder().pathField("  ").region("us-east-1").build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }

  @Test
  void rejectsBlankRegion() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder().pathField("s3Path").region("").build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }

  @Test
  void rejectsDeserializeWithoutEncodingType() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder()
            .pathField("s3Path")
            .region("us-east-1")
            .emission(S3FileReaderDto.Emission.DESERIALIZE)
            .encodingType(null)
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }
}
