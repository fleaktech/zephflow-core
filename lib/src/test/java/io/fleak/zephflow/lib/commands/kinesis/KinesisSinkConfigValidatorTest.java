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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
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
          .encodingType(EncodingType.JSON_OBJECT.toString())
          .build();

  @Test
  void validateConfig() {
    KinesisSinkConfigValidator validator = new KinesisSinkConfigValidator();
    validator.validateConfig(KINESIS_SINK_TEST_CONFIG, "myNodeId", KINESIS_SINK_TEST_JOB_CONTEXT);
  }
}
