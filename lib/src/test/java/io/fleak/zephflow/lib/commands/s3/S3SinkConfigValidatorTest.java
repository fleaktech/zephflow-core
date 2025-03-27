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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/6/24 */
class S3SinkConfigValidatorTest {

  @Test
  void validateConfig() {
    String configStr =
        """
      {
        "regionStr": "us-east-1",
        "bucketName": "example-bucket",
        "keyName": "example-key",
        "encodingType": "JSON_OBJECT",
        "credentialId": "credential_2"
      }
    """;
    S3SinkConfigValidator validator = new S3SinkConfigValidator();
    JsonConfigParser<S3SinkDto.Config> configParser =
        new JsonConfigParser<>(S3SinkDto.Config.class);

    S3SinkDto.Config config = configParser.parseConfig(configStr);
    validator.validateConfig(config, "abc", JOB_CONTEXT);
  }
}
