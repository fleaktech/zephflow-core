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
package io.fleak.zephflow.lib.commands.smtpsink;

import io.fleak.zephflow.api.CommandConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class SmtpSinkDto {

  public static final int DEFAULT_PORT = 587;
  public static final String DEFAULT_BODY_CONTENT_TYPE = "text/plain";
  public static final boolean DEFAULT_USE_TLS = true;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    private String host;
    @Builder.Default private Integer port = DEFAULT_PORT;
    private String credentialId;
    private String fromAddress;
    private String toTemplate;
    private String ccTemplate;
    private String subjectTemplate;
    private String bodyTemplate;
    @Builder.Default private String bodyContentType = DEFAULT_BODY_CONTENT_TYPE;
    @Builder.Default private Boolean useTls = DEFAULT_USE_TLS;
  }
}
