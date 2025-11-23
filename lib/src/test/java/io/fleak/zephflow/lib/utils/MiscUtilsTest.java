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
package io.fleak.zephflow.lib.utils;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Created by bolei on 7/5/24 */
class MiscUtilsTest {

  @Test
  void testCollectDelimitedTreeElements() {
    // Create a ParserRuleContext with 6 mock children
    ParserRuleContext context = mock(ParserRuleContext.class);
    when(context.getChildCount()).thenReturn(6);
    for (int i = 0; i < 6; i++) {
      ParseTree child = mock(ParseTree.class);
      when(context.getChild(i)).thenReturn(child);
    }

    // Call the method
    List<ParseTree> result = collectDelimitedTreeElements(context);

    // Verify that result contains only the even-indexed children
    assertEquals(3, result.size());
    for (int i = 0; i < 3; i++) {
      assertEquals(context.getChild(i * 2), result.get(i));
    }
  }

  @Test
  public void TestLoadStringFromResourceIgnoreAllWhitespace() throws IOException {
    String inputEvents = MiscUtils.loadStringFromResource("/json/record_event_1.json");
    assertEquals(1028, inputEvents.getBytes().length);
  }

  @Test
  @DisplayName("should return user tag when no event provided")
  void getCallingUserTag_shouldReturnUserTag_whenNoEvent() {
    Map<String, String> result = getCallingUserTagAndEventTags("test-user", null);

    assertEquals(1, result.size());
    assertEquals("test-user", result.get(METRIC_TAG_CALLING_USER));
  }

  @Test
  @DisplayName("should extract event tags when event has tag field")
  void getCallingUserTag_shouldExtractEventTags_whenEventHasTagField() {
    var event =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "__tag__",
                    Map.of(
                        "tag_1", "tag_value_1",
                        "tag_2", "tag_value_2"),
                    "other_field",
                    "other_value"));

    Map<String, String> result = getCallingUserTagAndEventTags("test-user", event);

    assertEquals(3, result.size());
    assertEquals("test-user", result.get(METRIC_TAG_CALLING_USER));
    assertEquals("tag_value_1", result.get("tag_1"));
    assertEquals("tag_value_2", result.get("tag_2"));
  }

  @Test
  @DisplayName("should handle event without tag field")
  void getCallingUserTag_shouldHandleEventWithoutTagField() {
    var event =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "key1", "value1",
                    "key2", "value2"));

    Map<String, String> result = getCallingUserTagAndEventTags("test-user", event);

    assertEquals(1, result.size());
    assertEquals("test-user", result.get(METRIC_TAG_CALLING_USER));
  }

  @Test
  @DisplayName("should return empty map when no user and no event")
  void getCallingUserTag_shouldReturnEmptyMap_whenNoUserAndNoEvent() {
    Map<String, String> result = getCallingUserTagAndEventTags(null, null);

    assertTrue(result.isEmpty());
  }
}
