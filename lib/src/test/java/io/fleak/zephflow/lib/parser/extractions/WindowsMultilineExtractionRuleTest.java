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
import static io.fleak.zephflow.lib.utils.MiscUtils.loadStringFromResource;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import org.junit.jupiter.api.Test;

/** Created by bolei on 2/1/25 */
class WindowsMultilineExtractionRuleTest {
  @Test
  public void testExtract_tsFirstLine() throws Exception {
    String rawLog = loadStringFromResource("/parser/windows_multiline_4624.txt");

    WindowsMultilineExtractionRule rule =
        new WindowsMultilineExtractionRule(
            new WindowsMultilineExtractionRule.FirstLineTimestampExtractor());
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
            new WindowsMultilineExtractionRule.FromFieldTimestampExtractor("Date"));
    FleakData result = rule.extract(rawLog);
    FleakData expected =
        loadFleakDataFromJsonResource("/parser/windows_multiline_4624_parsed_ts_in_field.json");
    assertEquals(expected, result);
  }

  @Test
  public void testExtract_noTs() throws Exception {
    String rawLog = loadStringFromResource("/parser/windows_multiline_4624_no_timestamp.txt");

    WindowsMultilineExtractionRule rule = new WindowsMultilineExtractionRule(null); // no timestamp
    FleakData result = rule.extract(rawLog);

    FleakData expected =
        loadFleakDataFromJsonResource("/parser/windows_multiline_4624_no_timestamp_parsed.json");
    assertEquals(expected, result);
  }
}
