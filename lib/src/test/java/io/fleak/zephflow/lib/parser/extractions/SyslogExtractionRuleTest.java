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
import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionRule.LOG_CONTENT_KEY;
import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.ParserConfigCompiler;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;

/** Created by bolei on 2/24/25 */
class SyslogExtractionRuleTest {

  @Test
  void extract() throws Exception {
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField(FIELD_NAME_RAW)
            .extractionConfig(
                SyslogExtractionConfig.builder()
                    .componentList(List.of(PRIORITY, TIMESTAMP, DEVICE))
                    .timestampPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .build())
            .dispatchConfig(
                ParserConfigs.DispatchConfig.builder()
                    .dispatchField(LOG_CONTENT_KEY)
                    .defaultConfig(
                        ParserConfigs.ParserConfig.builder()
                            .targetField(LOG_CONTENT_KEY)
                            .removeTargetField(true)
                            .extractionConfig(new CefExtractionConfig())
                            .build())
                    .build())
            .build();

    String raw = MiscUtils.loadStringFromResource("/parser/syslog_cef_1.txt");
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of(FIELD_NAME_RAW, raw));
    assert input != null;
    FleakData actual = new ParserConfigCompiler().compile(parserConfig).parse(input);

    FleakData expected = loadFleakDataFromJsonResource("/parser/syslog_cef_1_parsed.json");
    assertEquals(expected, actual);
  }

  @Test
  public void testParseSyslogHeaders() throws IOException {
    List<TestCase> testCases =
        fromJsonResource("/parser/syslog_header_test_cases.json", new TypeReference<>() {});

    testCases.forEach(
        tc -> {
          SyslogExtractionRule parser = new SyslogExtractionRule(tc.config);
          RecordFleakData actual;
          try {
            actual = parser.extract(tc.logEntry);
            System.out.println(toJsonString(actual));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          assertEquals(tc.expected, actual.unwrap());
        });
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder
  private static class TestCase {
    String logEntry;
    SyslogExtractionConfig config;
    Map<String, Object> expected;
  }
}
