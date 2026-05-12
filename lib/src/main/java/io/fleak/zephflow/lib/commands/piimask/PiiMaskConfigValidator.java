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
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.CustomPattern;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class PiiMaskConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    Config c = (Config) commandConfig;

    Preconditions.checkArgument(
        c.targets() != null && !c.targets().isEmpty(),
        "piimask: 'targets' must contain at least one path");

    for (String t : c.targets()) {
      try {
        DottedPath.parse(t);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("piimask: " + e.getMessage(), e);
      }
    }

    boolean anyBuiltInActive = hasActiveBuiltIn(c.detectors());
    boolean anyCustom = c.customPatterns() != null && !c.customPatterns().isEmpty();
    Preconditions.checkArgument(
        anyBuiltInActive || anyCustom,
        "piimask: at least one built-in detector or one customPatterns entry must be configured");

    if (anyCustom) {
      Set<String> seenNames = new HashSet<>();
      for (CustomPattern cp : c.customPatterns()) {
        Preconditions.checkArgument(
            cp.name() != null && !cp.name().isBlank(),
            "piimask: customPatterns entries must have a non-blank 'name'");
        Preconditions.checkArgument(
            cp.pattern() != null && !cp.pattern().isBlank(),
            "piimask: customPatterns entry '%s' must have a non-blank 'pattern'",
            cp.name());
        Preconditions.checkArgument(
            cp.replacement() != null && !cp.replacement().isBlank(),
            "piimask: customPatterns entry '%s' must have a non-blank 'replacement'",
            cp.name());
        Preconditions.checkArgument(
            seenNames.add(cp.name()), "piimask: duplicate customPatterns name '%s'", cp.name());
        try {
          Pattern.compile(cp.pattern());
        } catch (PatternSyntaxException e) {
          throw new IllegalArgumentException(
              "piimask: customPatterns entry '"
                  + cp.name()
                  + "' has invalid regex: "
                  + e.getMessage(),
              e);
        }
      }
    }
  }

  private static boolean hasActiveBuiltIn(Detectors d) {
    if (d == null) return false;
    return Arrays.stream(BuiltInDetectors.values()).anyMatch(bd -> d.get(bd) != null);
  }
}
