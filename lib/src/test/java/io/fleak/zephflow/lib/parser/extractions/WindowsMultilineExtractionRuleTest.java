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

import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonResource;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_TS;
import static io.fleak.zephflow.lib.utils.MiscUtils.loadStringFromResource;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.ParserConfigCompiler;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 2/1/25 */
class WindowsMultilineExtractionRuleTest {
  @Test
  public void testExtract_tsFirstLine() throws Exception {
    String rawLog = loadStringFromResource("/parser/windows_multiline_4624.txt");

    WindowsMultilineExtractionRule rule =
        new WindowsMultilineExtractionRule(
            new WindowsMultilineExtractionRule.FirstLineTimestampExtractor(), ZoneOffset.UTC);
    FleakData result = rule.extract(rawLog);
    FleakData expected =
        loadFleakDataFromJsonResource("/parser/windows_multiline_4624_parsed.json");
    assertEquals(expected, result);
  }

  @Test
  public void testExtract_tsFromField() throws Exception {
    String rawLog = loadStringFromResource("/parser/windows_multiline_4624_ts_in_field.txt");

    WindowsMultilineExtractionRule rule =
        new WindowsMultilineExtractionRule(
            new WindowsMultilineExtractionRule.FromFieldTimestampExtractor("Date"), ZoneOffset.UTC);
    FleakData result = rule.extract(rawLog);
    FleakData expected =
        loadFleakDataFromJsonResource("/parser/windows_multiline_4624_parsed_ts_in_field.json");
    assertEquals(expected, result);
  }

  @Test
  public void testExtract_noTs() throws Exception {
    String rawLog = loadStringFromResource("/parser/windows_multiline_4624_no_timestamp.txt");

    WindowsMultilineExtractionRule rule =
        new WindowsMultilineExtractionRule(null, ZoneOffset.UTC); // no timestamp
    FleakData result = rule.extract(rawLog);

    FleakData expected =
        loadFleakDataFromJsonResource("/parser/windows_multiline_4624_no_timestamp_parsed.json");
    assertEquals(expected, result);
  }

  @Test
  public void testExtract_windowsEventViewer() throws Exception {
    String rawLog =
        loadStringFromResource("/parser/windows_multiline_4624_windows_eventviewer.txt");
    WindowsMultilineExtractionRule rule =
        new WindowsMultilineExtractionRule(
            new WindowsMultilineExtractionRule.FromFieldTimestampExtractor("Date"), ZoneOffset.UTC);
    FleakData result = rule.extract(rawLog);
    FleakData expected =
        loadFleakDataFromJsonResource(
            "/parser/windows_multiline_4624_windows_eventviewer_parsed.json");
    assertEquals(expected, result);
  }

  @Test
  public void testExtract_windowsEventViewer_List() throws Exception {
    String rawLog =
        loadStringFromResource("/parser/windows_multiline_4672_windows_eventviewer.txt");
    WindowsMultilineExtractionRule rule =
        new WindowsMultilineExtractionRule(
            new WindowsMultilineExtractionRule.FromFieldTimestampExtractor("Date"), ZoneOffset.UTC);
    FleakData result = rule.extract(rawLog);
    FleakData expected =
        loadFleakDataFromJsonResource(
            "/parser/windows_multiline_4672_windows_eventviewer_parsed.json");
    assertEquals(expected, result);
  }

  @Test
  public void testCompiledConfig_defaultTimezoneIsUtc() throws Exception {
    RecordFleakData result =
        parseWithCompiledConfig(null, "03/12/2021 10:48:14 AM\nLogName=Security");
    assertEquals(1615546094000L, result.getPayload().get(FIELD_NAME_TS).getLongValue());
  }

  @Test
  public void testCompiledConfig_namedTimezone() throws Exception {
    RecordFleakData result =
        parseWithCompiledConfig("America/Chicago", "03/12/2021 10:48:14 AM\nLogName=Security");
    assertEquals(1615567694000L, result.getPayload().get(FIELD_NAME_TS).getLongValue());
  }

  @Test
  public void testCompiledConfig_fixedOffsetTimezone() throws Exception {
    RecordFleakData result =
        parseWithCompiledConfig("+02:00", "03/12/2021 10:48:14 AM\nLogName=Security");
    assertEquals(1615538894000L, result.getPayload().get(FIELD_NAME_TS).getLongValue());
  }

  @Test
  public void testCompiledConfig_unparseableTimestampWritesNoTs() throws Exception {
    String raw = "Date=yesterday\nLogName=Security";
    RecordFleakData result = parseWithCompiledConfig(null, raw);
    assertFalse(result.getPayload().containsKey(FIELD_NAME_TS));
    assertEquals(raw, result.getPayload().get(FIELD_NAME_RAW).getStringValue());
    assertEquals("yesterday", result.getPayload().get("Date").getStringValue());
  }

  @Test
  public void testCompiledConfig_invalidTimezoneFailsCompilation() {
    ParserConfigs.ParserConfig parserConfig = firstLineConfig("America/Chicgo");
    DateTimeException e =
        assertThrows(
            DateTimeException.class, () -> new ParserConfigCompiler().compile(parserConfig));
    assertTrue(e.getMessage().contains("America/Chicgo"), e.getMessage());
  }

  private static RecordFleakData parseWithCompiledConfig(String timezone, String raw) {
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of(FIELD_NAME_RAW, raw));
    return new ParserConfigCompiler().compile(firstLineConfig(timezone)).parse(input);
  }

  private static ParserConfigs.ParserConfig firstLineConfig(String timezone) {
    return ParserConfigs.ParserConfig.builder()
        .targetField(FIELD_NAME_RAW)
        .extractionConfig(
            WindowsMultilineExtractionConfig.builder()
                .timestampLocationType(
                    WindowsMultilineExtractionConfig.TimestampLocationType.FIRST_LINE)
                .timezone(timezone)
                .build())
        .build();
  }
}
