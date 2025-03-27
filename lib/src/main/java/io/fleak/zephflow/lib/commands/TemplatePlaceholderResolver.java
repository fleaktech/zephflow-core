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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

/** Created by bolei on 8/28/24 */
@EqualsAndHashCode
public class TemplatePlaceholderResolver {
  private static final String PLACEHOLDER_PATTERN_STR = "\\{\\{\\s*(.*?)\\s*}}";
  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile(PLACEHOLDER_PATTERN_STR);

  private final String template;
  private final List<Placeholder> placeholders;

  private TemplatePlaceholderResolver(
      @NonNull String template, @NonNull List<Placeholder> placeholders) {
    this.template = template;
    this.placeholders = placeholders;
  }

  public static TemplatePlaceholderResolver create(String template) {
    List<Placeholder> placeholders = extractPlaceholders(template);
    return new TemplatePlaceholderResolver(template, placeholders);
  }

  public String resolvePlaceholders(FleakData event) {
    StringBuilder result = new StringBuilder();
    int lastPos = 0;

    for (Placeholder placeholder : placeholders) {
      // Append the part of the template before the current placeholder
      result.append(template, lastPos, placeholder.start);
      FleakData fleakData = placeholder.pathExpression.calculateValue(event);
      String replacement;
      if (fleakData == null) {
        replacement = null;
      } else if (fleakData instanceof RecordFleakData || fleakData instanceof ArrayFleakData) {
        replacement = toJsonString(fleakData.unwrap());
      } else {
        replacement = Objects.toString(fleakData.unwrap());
      }
      result.append(replacement);

      lastPos = placeholder.end;
    }

    // Append the remaining part of the template after the last placeholder
    result.append(template, lastPos, template.length());

    return result.toString();
  }

  private static List<Placeholder> extractPlaceholders(String template) {
    List<Placeholder> placeholders = new ArrayList<>();
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
    while (matcher.find()) {
      String pathExpStr = matcher.group(1);
      PathExpression pathExpression = PathExpression.fromString(pathExpStr);
      placeholders.add(new Placeholder(matcher.start(), matcher.end(), pathExpression));
    }
    return placeholders;
  }

  public record Placeholder(int start, int end, PathExpression pathExpression) {}
}
