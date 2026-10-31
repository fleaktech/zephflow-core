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
package io.fleak.zephflow.lib.commands.eval;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.auto.DetectedFormat;
import io.fleak.zephflow.lib.serdes.decode.auto.FormatDetector;
import io.fleak.zephflow.lib.serdes.decode.auto.FormatRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FormatFunctionsTest extends FeelFunctionTestBase {
  @Test
  void classificationAndProjectionPreserveOneArrayAndMetadata() {
    String text = "[1, {\"x\": 2}, false]";
    var input = FleakData.wrap(Map.of("__raw__", text, "key", "message-key"));
    var classified = (RecordFleakData) evaluateExpression("format_classify($, \"__raw__\")", input);
    assertEquals(DetectedFormat.JSON_ARRAY, FormatRecord.format(classified));
    assertEquals(
        Map.of("__raw__", text, "key", "message-key"),
        evaluateExpression("format_record($)", classified).unwrap());
  }

  @Test
  void projectedObjectSupportsExistingFieldFunctionsAndFormatFilters() {
    var envelope =
        FormatDetector.classify("{\"env\":\"prod\",\"__raw__\":\"a,b\",\"formatId\":\"csv\"}");
    assertEquals(
        true,
        evaluateExpression(FormatRecord.FORMAT_PATH + " == \"json_object\"", envelope).unwrap());
    var projected = evaluateExpression("format_record($)", envelope);
    assertEquals(
        true,
        evaluateExpression("field_value_eq($, \"env\", \"\\\"prod\\\"\")", projected).unwrap());
    assertEquals("csv", projected.getPayload().get("formatId").unwrap());
    assertEquals("a,b", projected.getPayload().get("__raw__").unwrap());
  }

  @Test
  void invalidRepresentationFailsRatherThanBecomingUnrecognized() {
    for (var input :
        List.of(FleakData.wrap(Map.of("__raw__", 1)), FleakData.wrap(Map.of("value", "text")))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> evaluateExpression("format_classify($, \"__raw__\")", input));
      assertThrows(
          IllegalArgumentException.class, () -> evaluateExpression("format_record($)", input));
    }
  }
}
