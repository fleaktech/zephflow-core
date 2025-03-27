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

import static io.fleak.zephflow.lib.utils.MiscUtils.collectDelimitedTreeElements;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
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
}
