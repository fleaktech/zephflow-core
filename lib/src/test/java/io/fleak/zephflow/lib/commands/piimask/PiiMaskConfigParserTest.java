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

import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiMaskConfigParserTest {

  private final PiiMaskConfigParser parser = new PiiMaskConfigParser();

  @Test
  void parsesTargetsOnly() {
    Config c = (Config) parser.parseConfig(Map.of("targets", List.of("$.foo")));
    assertEquals(List.of("$.foo"), c.targets());
    assertNull(c.detectors());
    assertNull(c.customPatterns());
  }

  @Test
  void parsesActiveBuiltInWithDefaultReplacement() {
    Config c =
        (Config)
            parser.parseConfig(
                Map.of(
                    "targets", List.of("$.x"),
                    "detectors", Map.of("email", Map.of())));
    assertNotNull(c.detectors());
    assertNotNull(c.detectors().email());
    assertNull(c.detectors().email().replacement());
    assertNull(c.detectors().phone());
  }

  @Test
  void parsesActiveBuiltInWithCustomReplacement() {
    Config c =
        (Config)
            parser.parseConfig(
                Map.of(
                    "targets", List.of("$.x"),
                    "detectors", Map.of("email", Map.of("replacement", "<E>"))));
    assertEquals("<E>", c.detectors().email().replacement());
  }

  @Test
  void detectorPresentWithNullMapDisablesIt() {
    java.util.Map<String, Object> root = new java.util.HashMap<>();
    root.put("targets", List.of("$.x"));
    java.util.Map<String, Object> det = new java.util.HashMap<>();
    det.put("email", null);
    det.put("phone", Map.of());
    root.put("detectors", det);
    Config c = (Config) parser.parseConfig(root);
    assertNull(c.detectors().email());
    assertNotNull(c.detectors().phone());
  }

  @Test
  void parsesCustomPatterns() {
    Config c =
        (Config)
            parser.parseConfig(
                Map.of(
                    "targets", List.of("$.x"),
                    "customPatterns",
                        List.of(
                            Map.of("name", "id", "pattern", "INT-\\d+", "replacement", "<ID>"))));
    assertEquals(1, c.customPatterns().size());
    CustomPattern cp = c.customPatterns().get(0);
    assertEquals("id", cp.name());
    assertEquals("INT-\\d+", cp.pattern());
    assertEquals("<ID>", cp.replacement());
  }

  @Test
  void targetsMustBePresent() {
    assertThrows(
        IllegalArgumentException.class,
        () -> parser.parseConfig(Map.of("detectors", Map.of("email", Map.of()))));
  }
}
