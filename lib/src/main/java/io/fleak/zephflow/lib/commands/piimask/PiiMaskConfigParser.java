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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PiiMaskConfigParser implements ConfigParser {

  @Override
  @SuppressWarnings("unchecked")
  public CommandConfig parseConfig(Map<String, Object> config) {
    Object rawTargets = config.get("targets");
    Preconditions.checkArgument(
        rawTargets instanceof List<?>, "piimask command requires 'targets' to be a list of paths");
    List<String> targets = new ArrayList<>();
    for (Object t : (List<Object>) rawTargets) {
      targets.add(String.valueOf(t));
    }

    Detectors detectors = null;
    Object rawDetectors = config.get("detectors");
    if (rawDetectors instanceof Map<?, ?> dm) {
      detectors =
          new Detectors(
              parseDetector(dm, "email"),
              parseDetector(dm, "phone"),
              parseDetector(dm, "ssn"),
              parseDetector(dm, "creditCard"),
              parseDetector(dm, "ipv4"),
              parseDetector(dm, "ipv6"));
    }

    List<CustomPattern> customPatterns = null;
    Object rawCustom = config.get("customPatterns");
    if (rawCustom instanceof List<?> cl) {
      customPatterns = new ArrayList<>();
      for (Object e : cl) {
        if (e instanceof Map<?, ?> em) {
          customPatterns.add(
              new CustomPattern(
                  stringOrNull(em.get("name")),
                  stringOrNull(em.get("pattern")),
                  stringOrNull(em.get("replacement"))));
        }
      }
    }

    return new Config(targets, detectors, customPatterns);
  }

  private static DetectorConfig parseDetector(Map<?, ?> detectors, String key) {
    if (!detectors.containsKey(key)) return null;
    Object v = detectors.get(key);
    if (!(v instanceof Map<?, ?> m)) return null;
    return new DetectorConfig(stringOrNull(m.get("replacement")));
  }

  private static String stringOrNull(Object v) {
    return v == null ? null : String.valueOf(v);
  }
}
