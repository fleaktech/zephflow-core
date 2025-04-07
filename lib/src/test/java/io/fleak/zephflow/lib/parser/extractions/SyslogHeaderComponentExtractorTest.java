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

import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionRule.TIMESTAMP_KEY;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/6/25 */
class SyslogHeaderComponentExtractorTest {
  @Test
  public void testTimestampExtractComponent() {
    Locale.setDefault(Locale.CHINESE); // use a locale that's other than US

    String input = "Oct 10 2018 12:34:56 my other text";
    SyslogHeaderComponentExtractor.TimestampComponentExtractor extractor =
        new SyslogHeaderComponentExtractor.TimestampComponentExtractor();
    Map<String, Object> result = new HashMap<>();
    int endPos = extractor.extractComponent(result, input, 0, true, null, "MMM dd yyyy HH:mm:ss");
    assertEquals(21, endPos);
    assertEquals(Map.of(TIMESTAMP_KEY, "Oct 10 2018 12:34:56"), result);
  }
}
