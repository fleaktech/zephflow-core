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
package io.fleak.zephflow.lib.commands.syslogudp;

import io.fleak.zephflow.api.CommandConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface SyslogUdpDto {

  String METADATA_SOURCE_ADDRESS = "source_address";
  String METADATA_SOURCE_PORT = "source_port";
  String METADATA_RECEIVED_AT = "received_at";

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @Builder.Default private String host = "0.0.0.0";
    @Builder.Default private int port = 514;
    @Builder.Default private int bufferSize = 65535;
    @Builder.Default private int queueCapacity = 10000;
    @Builder.Default private String encoding = "UTF-8";
  }
}
