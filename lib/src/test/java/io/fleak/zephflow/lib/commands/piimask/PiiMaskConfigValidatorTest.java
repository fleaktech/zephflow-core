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
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.List;
import org.junit.jupiter.api.Test;

class PiiMaskConfigValidatorTest {

  private final PiiMaskConfigValidator v = new PiiMaskConfigValidator();

  private static Detectors emailOnly() {
    return new Detectors(new DetectorConfig(null), null, null, null, null, null);
  }

  @Test
  void acceptsMinimalValidConfig() {
    Config c = new Config(List.of("$.foo"), emailOnly(), null);
    assertDoesNotThrow(() -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsEmptyTargets() {
    Config c = new Config(List.of(), emailOnly(), null);
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsMalformedTarget() {
    Config c = new Config(List.of("$.foo[0]"), emailOnly(), null);
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsConfigWithNoActiveDetectorsAndNoCustom() {
    Config c = new Config(List.of("$.foo"), null, null);
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void acceptsConfigWithOnlyCustomPatterns() {
    Config c =
        new Config(List.of("$.foo"), null, List.of(new CustomPattern("id", "X-\\d+", "<ID>")));
    assertDoesNotThrow(() -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsBlankCustomName() {
    Config c =
        new Config(List.of("$.foo"), null, List.of(new CustomPattern("  ", "X-\\d+", "<ID>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsBlankCustomPattern() {
    Config c = new Config(List.of("$.foo"), null, List.of(new CustomPattern("id", "", "<ID>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsBlankCustomReplacement() {
    Config c = new Config(List.of("$.foo"), null, List.of(new CustomPattern("id", "X-\\d+", null)));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsInvalidCustomRegex() {
    Config c = new Config(List.of("$.foo"), null, List.of(new CustomPattern("id", "(", "<ID>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }

  @Test
  void rejectsDuplicateCustomNames() {
    Config c =
        new Config(
            List.of("$.foo"),
            null,
            List.of(new CustomPattern("dup", "A", "<A>"), new CustomPattern("dup", "B", "<B>")));
    assertThrows(IllegalArgumentException.class, () -> v.validateConfig(c, "n1", null));
  }
}
