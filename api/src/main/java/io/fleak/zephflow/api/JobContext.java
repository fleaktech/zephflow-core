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
package io.fleak.zephflow.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 5/11/24 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobContext implements Serializable {
  private @Builder.Default Map<String, Serializable> otherProperties = new HashMap<>();
  private @Builder.Default Map<String, String> metricTags = new HashMap<>();

  private @Builder.Default DlqConfig dlqConfig = new LoggingDlqConfig();

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = S3DlqConfig.class, name = "s3"),
    @JsonSubTypes.Type(value = LoggingDlqConfig.class, name = "logging"),
  })
  public interface DlqConfig {}

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class S3DlqConfig implements DlqConfig {
    private String region;
    private String bucket;
    private int batchSize;
    private int flushIntervalMillis;
  }

  @Data
  @Builder
  public static class LoggingDlqConfig implements DlqConfig {}
}
