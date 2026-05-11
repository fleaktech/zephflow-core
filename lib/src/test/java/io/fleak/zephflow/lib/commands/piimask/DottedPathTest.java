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
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class DottedPathTest {

  @Test
  void parsesSingleSegment() {
    DottedPath p = DottedPath.parse("$.foo");
    assertEquals(List.of("foo"), p.segments());
  }

  @Test
  void parsesNestedPath() {
    DottedPath p = DottedPath.parse("$.user.email");
    assertEquals(List.of("user", "email"), p.segments());
  }

  @Test
  void parsesUnderscoresAndDigits() {
    DottedPath p = DottedPath.parse("$.user_1.field2");
    assertEquals(List.of("user_1", "field2"), p.segments());
  }

  @Test
  void rejectsMissingDollarPrefix() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("foo.bar"));
  }

  @Test
  void rejectsBracketIndexing() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$.foo[0]"));
  }

  @Test
  void rejectsWildcard() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$.foo.*"));
  }

  @Test
  void rejectsLeadingDigitSegment() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$.0bad"));
  }

  @Test
  void rejectsEmptyPath() {
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse("$"));
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse(""));
    assertThrows(IllegalArgumentException.class, () -> DottedPath.parse(null));
  }
}
