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

import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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

  /** How resolved values are encoded for the text the template produces. */
  public enum Escaping {
    NONE,
    /**
     * The template is a URL. Values after the scheme and host are percent-encoded. A placeholder
     * that starts the template or is the whole host is inserted verbatim; any other placeholder
     * inside the host must not contain characters that change which host is addressed.
     */
    URL
  }

  /** What {@link #resolvePlaceholders} does when a placeholder resolves to no value. */
  public enum MissingValue {
    /** Return null for the whole template so the caller can detect the missing field. */
    RETURN_NULL,
    EMPTY
  }

  private enum Encoding {
    VERBATIM,
    PERCENT_ENCODED,
    HOST_SEGMENT
  }

  private static final String HOST_SEGMENT_FORBIDDEN = "/?#@\\:";

  private final String template;
  private final List<Placeholder> placeholders;
  private final List<Encoding> encodings;
  private final MissingValue missingValue;

  private TemplatePlaceholderResolver(
      @NonNull String template,
      @NonNull List<Placeholder> placeholders,
      @NonNull Escaping escaping,
      @NonNull MissingValue missingValue) {
    this.template = template;
    this.placeholders = placeholders;
    this.encodings =
        escaping == Escaping.URL
            ? urlEncodings(template, placeholders)
            : placeholders.stream().map(placeholder -> Encoding.VERBATIM).toList();
    this.missingValue = missingValue;
  }

  public static TemplatePlaceholderResolver create(String template) {
    return create(template, Escaping.NONE, MissingValue.RETURN_NULL);
  }

  public static TemplatePlaceholderResolver create(
      String template, Escaping escaping, MissingValue missingValue) {
    return new TemplatePlaceholderResolver(
        template, extractPlaceholders(template, null), escaping, missingValue);
  }

  /** Locates an invalid placeholder in the configuration field containing its template. */
  public static TemplatePlaceholderResolver create(String template, ConfigurationPath path) {
    return create(template, path, Escaping.NONE, MissingValue.RETURN_NULL);
  }

  public static TemplatePlaceholderResolver create(
      String template, ConfigurationPath path, Escaping escaping, MissingValue missingValue) {
    try {
      return new TemplatePlaceholderResolver(
          template, extractPlaceholders(template, path), escaping, missingValue);
    } catch (NullPointerException failure) {
      if (template == null) {
        throw ConfigurationValidationException.at(path, failure, "Template must not be null.");
      }
      throw failure;
    }
  }

  /**
   * Resolves placeholders against the given event.
   *
   * @return the rendered string, or {@code null} if any placeholder's path expression resolves to
   *     null on the event and the resolver was created with {@link MissingValue#RETURN_NULL} (the
   *     default). Callers can use a null return to detect missing input fields rather than silently
   *     rendering them as the literal string "null".
   * @throws TemplateValueException if a value would change which host a URL template addresses
   */
  public String resolvePlaceholders(FleakData event) {
    return resolve(event, false);
  }

  /**
   * Resolves placeholders against the given event, failing on the first placeholder that has no
   * value.
   *
   * @throws TemplateValueException naming the placeholder that has no value, or whose value would
   *     change which host a URL template addresses
   */
  public String resolveRequired(FleakData event) {
    return resolve(event, true);
  }

  List<Placeholder> placeholders() {
    return placeholders;
  }

  private String resolve(FleakData event, boolean required) {
    StringBuilder result = new StringBuilder();
    int lastPos = 0;

    for (int i = 0; i < placeholders.size(); i++) {
      Placeholder placeholder = placeholders.get(i);
      result.append(template, lastPos, placeholder.start);
      FleakData fleakData = placeholder.pathExpression.calculateValue(event);
      if (fleakData == null) {
        if (required) {
          throw new TemplateValueException(
              "Template placeholder '" + placeholder.expression + "' has no value on the event.");
        }
        if (missingValue == MissingValue.RETURN_NULL) {
          return null;
        }
      } else {
        result.append(encode(placeholder, encodings.get(i), text(fleakData)));
      }
      lastPos = placeholder.end;
    }

    result.append(template, lastPos, template.length());

    return result.toString();
  }

  static String text(FleakData fleakData) {
    if (fleakData instanceof RecordFleakData || fleakData instanceof ArrayFleakData) {
      return toJsonString(fleakData.unwrap());
    }
    return Objects.toString(fleakData.unwrap());
  }

  private static String encode(Placeholder placeholder, Encoding encoding, String value) {
    return switch (encoding) {
      case VERBATIM -> value;
      case PERCENT_ENCODED ->
          URLEncoder.encode(value, StandardCharsets.UTF_8)
              .replace("+", "%20")
              .replace("*", "%2A")
              .replace("%7E", "~");
      case HOST_SEGMENT -> {
        if (value
            .chars()
            .anyMatch(c -> HOST_SEGMENT_FORBIDDEN.indexOf(c) >= 0 || Character.isWhitespace(c))) {
          throw new TemplateValueException(
              "Template placeholder '"
                  + placeholder.expression
                  + "' is part of the URL host, and its value contains a character that would"
                  + " change the host.");
        }
        yield value;
      }
    };
  }

  /**
   * Decides each placeholder's encoding from the template's literal text. The host ends at the
   * first '/', '?' or '#' after any "://"; later placeholders are percent-encoded. Before that, a
   * placeholder that starts the template or is the entire host is verbatim, and any other one is a
   * host segment whose value is checked.
   */
  private static List<Encoding> urlEncodings(String template, List<Placeholder> placeholders) {
    char[] literal = template.toCharArray();
    for (Placeholder placeholder : placeholders) {
      Arrays.fill(literal, placeholder.start, placeholder.end, '\0');
    }
    String literalText = new String(literal);
    int separator = literalText.indexOf("://");
    // A "://" after the path starts belongs to a path or query value, not the scheme.
    boolean hasScheme = separator >= 0 && separator < firstPathDelimiter(literalText, 0);
    int hostStart = hasScheme ? separator + 3 : 0;
    int boundary = firstPathDelimiter(literalText, hostStart);
    return placeholders.stream()
        .map(
            placeholder -> {
              if (placeholder.start >= boundary) {
                return Encoding.PERCENT_ENCODED;
              }
              boolean startsTemplate = placeholder.start == 0;
              boolean wholeHost =
                  hasScheme && placeholder.start == hostStart && placeholder.end == boundary;
              return startsTemplate || wholeHost ? Encoding.VERBATIM : Encoding.HOST_SEGMENT;
            })
        .toList();
  }

  /** Index of the first '/', '?' or '#' at or after {@code from}, or the text length if none. */
  private static int firstPathDelimiter(String literalText, int from) {
    for (int i = from; i < literalText.length(); i++) {
      char c = literalText.charAt(i);
      if (c == '/' || c == '?' || c == '#') {
        return i;
      }
    }
    return literalText.length();
  }

  private static List<Placeholder> extractPlaceholders(String template, ConfigurationPath path) {
    List<Placeholder> placeholders = new ArrayList<>();
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
    while (matcher.find()) {
      String pathExpStr = matcher.group(1);
      PathExpression pathExpression;
      try {
        pathExpression =
            path == null
                ? PathExpression.fromString(pathExpStr)
                : PathExpression.fromString(pathExpStr, path);
      } catch (ConfigurationValidationException failure) {
        throw new ConfigurationValidationException(
            failure.originalFailure(),
            failure.issues().stream()
                .map(
                    issue ->
                        new ConfigurationIssue(
                            issue.path(),
                            "Template placeholder starting at character "
                                + matcher.start()
                                + ": "
                                + issue.message()))
                .toList());
      }
      placeholders.add(new Placeholder(matcher.start(), matcher.end(), pathExpression, pathExpStr));
    }
    return placeholders;
  }

  public record Placeholder(int start, int end, PathExpression pathExpression, String expression) {}
}
