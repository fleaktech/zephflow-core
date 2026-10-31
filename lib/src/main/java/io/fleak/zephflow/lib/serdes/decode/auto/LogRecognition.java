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
package io.fleak.zephflow.lib.serdes.decode.auto;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class LogRecognition {
  private static final Pattern RFC5424 =
      Pattern.compile(
          "(?s)^<(0|[1-9][0-9]{0,2})>([1-9][0-9]{0,2}) ([^ ]{1,32}) ([!-~]{1,255}) ([!-~]{1,48}) ([!-~]{1,128}) ([!-~]{1,32}) (.*)$");
  private static final Pattern TIMESTAMP =
      Pattern.compile(
          "[0-9]{4}-[0-9]{2}-[0-9]{2}T([0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?)(Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])");
  private static final Pattern BSD =
      Pattern.compile(
          "(?s)^(?:<(0|[1-9][0-9]{0,2})>)?(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) {1,2}([0-9]{1,2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}) ([!-~]+) (.*)$");
  private static final Pattern TAG =
      Pattern.compile("^[A-Za-z0-9_./-]+(?:\\[[0-9]+\\])?:\\s+(.*)$", Pattern.DOTALL);
  private static final List<String> MONTHS =
      List.of("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");
  private static final Set<String> SEVERITIES =
      Set.of("unknown", "low", "medium", "high", "very-high");

  private LogRecognition() {}

  static String syslogBody(String text) {
    Matcher modern = RFC5424.matcher(text);
    if (modern.matches() && Integer.parseInt(modern.group(1)) <= 191) {
      try {
        if (!modern.group(3).equals("-")) {
          Matcher timestamp = TIMESTAMP.matcher(modern.group(3));
          if (!timestamp.matches()) return null;
          LocalDate.parse(modern.group(3).substring(0, 10));
          LocalTime.parse(timestamp.group(1));
        }
        return structuredBody(modern.group(8));
      } catch (DateTimeParseException exception) {
        return null;
      }
    }
    Matcher legacy = BSD.matcher(text);
    if (!legacy.matches() || (legacy.group(1) != null && Integer.parseInt(legacy.group(1)) > 191)) {
      return null;
    }
    try {
      LocalDate.of(2000, MONTHS.indexOf(legacy.group(2)) + 1, Integer.parseInt(legacy.group(3)));
      LocalTime.parse(legacy.group(4));
      String body = legacy.group(6);
      Matcher tag = TAG.matcher(body);
      return tag.matches() ? tag.group(1) : body;
    } catch (java.time.DateTimeException exception) {
      return null;
    }
  }

  private static String structuredBody(String text) {
    if (text.equals("-")) return "";
    if (text.startsWith("- ")) return text.substring(2);
    int position = 0;
    Set<String> identifiers = new HashSet<>();
    while (position < text.length() && text.charAt(position) == '[') {
      int start = ++position;
      while (position < text.length() && nameCharacter(text.charAt(position))) position++;
      if (position == start
          || position - start > 32
          || !identifiers.add(text.substring(start, position))) return null;
      while (position < text.length() && text.charAt(position) == ' ') {
        start = ++position;
        while (position < text.length() && nameCharacter(text.charAt(position))) position++;
        if (position == start
            || position - start > 32
            || position + 1 >= text.length()
            || text.charAt(position) != '='
            || text.charAt(position + 1) != '"') return null;
        position += 2;
        boolean ended = false;
        while (position < text.length()) {
          char character = text.charAt(position++);
          if (character == '"') {
            ended = true;
            break;
          }
          if (character == ']') return null;
          if (character == '\\') {
            if (position < text.length() && "\\\"]".indexOf(text.charAt(position)) >= 0) position++;
          }
        }
        if (!ended) return null;
      }
      if (position == text.length() || text.charAt(position++) != ']') return null;
    }
    if (position == 0) return null;
    return position == text.length()
        ? ""
        : text.charAt(position) == ' ' ? text.substring(position + 1) : null;
  }

  private static boolean nameCharacter(char character) {
    return character >= '!'
        && character <= '~'
        && character != '='
        && character != '"'
        && character != ']';
  }

  static boolean cef(String text) {
    if (!text.startsWith("CEF:")) return false;
    List<String> fields = new ArrayList<>();
    StringBuilder field = new StringBuilder();
    int position = 4;
    while (position < text.length() && fields.size() < 7) {
      char character = text.charAt(position++);
      if (character == '\\') {
        if (position == text.length()) return false;
        char escaped = text.charAt(position++);
        if (escaped != '\\' && escaped != '|') return false;
        field.append(escaped);
      } else if (character == '|') {
        fields.add(field.toString());
        field.setLength(0);
      } else {
        if (character == '\n' || character == '\r') return false;
        field.append(character);
      }
    }
    if (fields.size() != 7
        || !fields.getFirst().matches("[0-9]+")
        || fields.subList(1, 6).stream().anyMatch(String::isEmpty)) return false;
    String severity = fields.get(6);
    if (!severity.matches("(?:[0-9]|10)")
        && !SEVERITIES.contains(severity.toLowerCase(java.util.Locale.ROOT))) return false;
    String extension = text.substring(position);
    if (extension.isBlank()) return true;
    return extension(extension);
  }

  private static boolean extension(String text) {
    int position = 0;
    while (position < text.length() && text.charAt(position) == ' ') position++;
    int keyStart = position;
    while (position < text.length() && extensionKey(text.charAt(position))) position++;
    if (position == keyStart || position == text.length() || text.charAt(position++) != '=')
      return false;
    int valueStart = position;
    while (position < text.length()) {
      char character = text.charAt(position++);
      if (character == '\n' || character == '\r') return false;
      if (character == '\\') {
        if (position == text.length() || "\\=nr".indexOf(text.charAt(position++)) < 0) return false;
      } else if (character == '=') {
        int start = position - 2;
        while (start >= valueStart && extensionKey(text.charAt(start))) start--;
        if (start < valueStart || text.charAt(start) != ' ' || start == position - 2) return false;
        valueStart = position;
      }
    }
    return true;
  }

  private static boolean extensionKey(char character) {
    return character >= 'A' && character <= 'Z'
        || character >= 'a' && character <= 'z'
        || character >= '0' && character <= '9'
        || character == '_'
        || character == '.';
  }
}
