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

import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionConfig.ComponentType.*;
import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionRule.*;
import static io.fleak.zephflow.lib.parser.extractions.SyslogHeaderComponentExtractor.*;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Enhanced configuration for parsing syslog headers.
 *
 * <p>Created by bolei on 2/24/25
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SyslogExtractionConfig implements ExtractionConfig {

  static final Map<ComponentType, SyslogHeaderComponentExtractor> EXTRACTOR_MAP =
      ImmutableMap.<ComponentType, SyslogHeaderComponentExtractor>builder()
          .put(PRIORITY, new SyslogHeaderComponentExtractor.PriorityComponentExtractor())
          .put(
              VERSION,
              new SyslogHeaderComponentExtractor.NoWhitespaceComponentExtractor(VERSION_KEY))
          .put(TIMESTAMP, new SyslogHeaderComponentExtractor.TimestampComponentExtractor())
          .put(
              DEVICE, new SyslogHeaderComponentExtractor.NoWhitespaceComponentExtractor(DEVICE_KEY))
          .put(APP, new SyslogHeaderComponentExtractor.NoWhitespaceComponentExtractor(APP_NAME_KEY))
          .put(PROC_ID, new NoWhitespaceComponentExtractor(PROC_ID_KEY))
          .put(MSG_ID, new NoWhitespaceComponentExtractor(MSG_ID_KEY))
          .put(STRUCTURED_DATA, new StructuredDataComponentExtractor())
          .build();

  public enum ComponentType {
    PRIORITY,
    VERSION,
    TIMESTAMP,
    DEVICE,
    APP,
    PROC_ID,
    MSG_ID,
    STRUCTURED_DATA
  }

  String timestampPattern;

  List<ComponentType> componentList;

  // Component presence configuration
  @Builder.Default private Character messageBodyDelimiter = null;
}
