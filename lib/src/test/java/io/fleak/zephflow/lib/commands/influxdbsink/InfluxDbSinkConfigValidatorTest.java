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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import org.junit.jupiter.api.Test;

class InfluxDbSinkConfigValidatorTest {

  private final InfluxDbSinkConfigValidator validator = new InfluxDbSinkConfigValidator();
  private final JobContext jobContext = mock(JobContext.class);

  private static InfluxDbSinkDto.Config.ConfigBuilder validConfig() {
    return InfluxDbSinkDto.Config.builder()
        .url("http://localhost:8086")
        .org("my-org")
        .bucket("my-bucket")
        .measurement("m");
  }

  private void validate(InfluxDbSinkDto.Config config) {
    validator.validateConfig(config, "node", jobContext);
  }

  @Test
  void acceptsValidConfig() {
    assertDoesNotThrow(() -> validate(validConfig().build()));
  }

  @Test
  void acceptsMeasurementFieldInsteadOfLiteral() {
    assertDoesNotThrow(
        () -> validate(validConfig().measurement(null).measurementField("type").build()));
  }

  @Test
  void rejectsMissingUrl() {
    var config = validConfig().url(" ").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsMissingOrg() {
    var config = validConfig().org(null).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsMissingBucket() {
    var config = validConfig().bucket("").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsNeitherMeasurementNorMeasurementField() {
    var config = validConfig().measurement(null).measurementField(null).build();
    assertTrue(
        assertThrows(IllegalArgumentException.class, () -> validate(config))
            .getMessage()
            .toLowerCase()
            .contains("measurement"));
  }

  @Test
  void rejectsBothMeasurementAndMeasurementField() {
    var config = validConfig().measurement("m").measurementField("type").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsNonPositiveBatchSize() {
    var config = validConfig().batchSize(0).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }
}
