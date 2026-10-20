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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkHecSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(Map.of("hec_token", new HashMap<>(Map.of("key", "abcd-1234")))))
          .build();

  private final SplunkHecSinkConfigValidator validator = new SplunkHecSinkConfigValidator();

  private SplunkHecSinkDto.Config validConfig() {
    return SplunkHecSinkDto.Config.builder()
        .hecUrl("https://splunk:8088/services/collector/event")
        .credentialId("hec_token")
        .index("security_logs")
        .sourcetype("paloalto:traffic")
        .build();
  }

  @Test
  void validateConfig_minimalValid() {
    SplunkHecSinkDto.Config c =
        SplunkHecSinkDto.Config.builder()
            .hecUrl("https://splunk:8088/services/collector/event")
            .credentialId("hec_token")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_fullyPopulatedValid() {
    assertDoesNotThrow(() -> validator.validateConfig(validConfig(), "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankHecUrl() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_malformedHecUrl() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("not a url");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_nonHttpScheme() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("ftp://splunk:8088/services/collector/event");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankCredentialId() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setCredentialId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeZero() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setBatchSize(0);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeOne_isValid() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setBatchSize(1);
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_jsonObjectEncodingValid() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("JSON_OBJECT");
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_jsonObjectWithCsvColumnsIgnoresColumns() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("JSON_OBJECT");
    c.setCsvColumns(List.of("time", "time"));
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_csvWithColumnsValid() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("CSV");
    c.setCsvColumns(List.of("time", "src_ip", "action"));
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_csvWithoutColumns() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("CSV");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
    assertEquals("csvColumns is required when encodingType is CSV", e.getMessage());
  }

  @Test
  void validateConfig_csvWithDuplicateColumns() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("CSV");
    c.setCsvColumns(List.of("time", "time"));
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_unsupportedEncodingType() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("JSON_ARRAY");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_unknownEncodingType() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setEncodingType("NOT_A_FORMAT");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }
}
