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
package io.fleak.zephflow.lib.commands.sqssource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.*;

public interface SqsSourceDto {

  int DEFAULT_MAX_NUMBER_OF_MESSAGES = 10;
  int MAX_MAX_NUMBER_OF_MESSAGES = 10;
  int DEFAULT_WAIT_TIME_SECONDS = 20;
  int MAX_WAIT_TIME_SECONDS = 20;
  int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @NonNull private String queueUrl;
    @NonNull private String regionStr;
    @NonNull private EncodingType encodingType;
    private String credentialId;
    @Builder.Default private Integer maxNumberOfMessages = DEFAULT_MAX_NUMBER_OF_MESSAGES;
    @Builder.Default private Integer waitTimeSeconds = DEFAULT_WAIT_TIME_SECONDS;
    @Builder.Default private Integer visibilityTimeoutSeconds = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;
  }
}
