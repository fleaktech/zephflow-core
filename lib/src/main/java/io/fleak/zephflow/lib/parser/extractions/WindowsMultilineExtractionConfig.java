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
package io.fleak.zephflow.lib.parser.extractions;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 2/1/25 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WindowsMultilineExtractionConfig implements ExtractionConfig {

  TimestampLocationType timestampLocationType;

  enum TimestampLocationType {
    NO_TIMESTAMP,
    FIRST_LINE,
    FROM_FIELD
  }

  Map<String, Object> config;
}
