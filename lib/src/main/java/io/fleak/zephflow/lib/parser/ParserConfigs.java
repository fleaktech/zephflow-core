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
package io.fleak.zephflow.lib.parser;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.parser.extractions.ExtractionConfig;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 10/14/24 */
public interface ParserConfigs {
  @Data
  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  class ParserConfig implements CommandConfig {
    private String targetField; // field to look at. Must be a string field
    private boolean removeTargetField;
    private ExtractionConfig extractionConfig; // rule to apply to extract fields from the string
    private DispatchConfig dispatchConfig; // further parsing
  }

  @Data
  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  class DispatchConfig {
    private String dispatchField;
    @Builder.Default private Map<String, ParserConfig> dispatchMap = Map.of();
    private ParserConfig defaultConfig;
  }
}
