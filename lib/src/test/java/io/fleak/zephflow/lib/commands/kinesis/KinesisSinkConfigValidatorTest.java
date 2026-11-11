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
package io.fleak.zephflow.lib.commands.kinesis;

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.encoding;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/5/24 */
class KinesisSinkConfigValidatorTest {
  static final JobContext KINESIS_SINK_TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "example-credential-id",
                      new HashMap<>(
                          Map.of("username", "test-access-key", "password", "test-secret-key")))))
          .build();
  static final KinesisSinkDto.Config KINESIS_SINK_TEST_CONFIG =
      KinesisSinkDto.Config.builder()
          .regionStr("us-west-2")
          .streamName("test-stream")
          .credentialId("example-credential-id")
          .partitionKeyFieldExpressionStr("$.partitionKey")
          .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
          .build();

  @Test
  void validateConfig() {
    KinesisSinkConfigValidator validator = new KinesisSinkConfigValidator();
    validator.validateConfig(KINESIS_SINK_TEST_CONFIG, "myNodeId", KINESIS_SINK_TEST_JOB_CONTEXT);
  }

  private static KinesisSinkDto.Config.ConfigBuilder withoutEncoding() {
    return KinesisSinkDto.Config.builder()
        .regionStr("us-west-2")
        .streamName("test-stream")
        .credentialId("example-credential-id");
  }

  @Test
  void validateConfig_encodingAlone() {
    KinesisSinkDto.Config config =
        withoutEncoding()
            .encoding(encoding("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"))
            .build();

    assertDoesNotThrow(
        () ->
            new KinesisSinkConfigValidator()
                .validateConfig(config, "myNodeId", KINESIS_SINK_TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_encodingInAFormatItCannotWrite() {
    KinesisSinkDto.Config config =
        withoutEncoding().encoding(encoding("{\"format\":{\"type\":\"kv\"}}")).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new KinesisSinkConfigValidator()
                    .validateConfig(config, "myNodeId", KINESIS_SINK_TEST_JOB_CONTEXT));
    assertEquals("kinesissink cannot write kv", exception.getMessage());
  }

  @Test
  void validateConfig_missingEncoding() {
    KinesisSinkDto.Config config = withoutEncoding().build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new KinesisSinkConfigValidator()
                    .validateConfig(config, "myNodeId", KINESIS_SINK_TEST_JOB_CONTEXT));
    assertEquals("kinesissink: encoding is required", exception.getMessage());
  }
}
