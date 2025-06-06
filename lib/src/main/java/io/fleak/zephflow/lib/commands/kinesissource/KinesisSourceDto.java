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
package io.fleak.zephflow.lib.commands.kinesissource;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.net.URI;
import java.util.Date;
import java.util.List;
import lombok.*;
import software.amazon.kinesis.common.InitialPositionInStream;

public interface KinesisSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @NonNull private String regionStr;
    @NonNull private String streamName;
    @NonNull private String applicationName;
    @NonNull private EncodingType encodingType;

    private List<CompressionType> compressionTypes;
    private InitialPositionInStream initialPosition;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private Date initialPositionTimestamp;

    private boolean disableMetrics;
    private URI dynamoEndpoint;
    private URI kinesisEndpoint;
    private URI cloudWatchEndpoint;

    private String credentialId;
  }
}
