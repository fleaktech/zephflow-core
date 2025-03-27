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

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.CompiledRules;
import io.fleak.zephflow.lib.parser.ParserConfigCompiler;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 2/26/25 */
class PanwTrafficExtractionRuleTest {
  @Test
  public void test() throws IOException {
    String raw = MiscUtils.loadStringFromResource("/parser/panw_traffic.txt");
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of(FIELD_NAME_RAW, raw));
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField(FIELD_NAME_RAW)
            .extractionConfig(
                SyslogExtractionConfig.builder()
                    .componentList(
                        List.of(
                            SyslogExtractionConfig.ComponentType.PRIORITY,
                            SyslogExtractionConfig.ComponentType.TIMESTAMP,
                            SyslogExtractionConfig.ComponentType.DEVICE))
                    .timestampPattern("MMM dd HH:mm:ss")
                    .build())
            .dispatchConfig(
                ParserConfigs.DispatchConfig.builder()
                    .dispatchField("content")
                    .defaultConfig(
                        ParserConfigs.ParserConfig.builder()
                            .removeTargetField(true)
                            .targetField("content")
                            .extractionConfig(new PanwTrafficExtractionConfig())
                            .build())
                    .build())
            .build();
    CompiledRules.ParseRule parseRule = new ParserConfigCompiler().compile(parserConfig);
    assert input != null;
    FleakData actual = parseRule.parse(input);
    FleakData expected = loadFleakDataFromJsonResource("/parser/panw_traffic_parsed.json");
    assertEquals(expected, actual);
  }
}
