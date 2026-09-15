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

import org.apache.commons.lang3.StringUtils;

/**
 * Validates and quotes SQL identifiers (schema, table and column names) that have to be
 * interpolated into statement text.
 *
 * <p>JDBC bind parameters can only carry values, never identifiers, so a sink that writes to a
 * user-configured table with column names taken from event payloads has no choice but to build
 * those parts of the statement as text. This class is the single place that is allowed to do it.
 *
 * <p>The safety argument is narrow on purpose. Inside a double-quoted identifier the double quote
 * is the only character with any meaning to the parser — nothing else can end the identifier or
 * start a new token. So an identifier that provably contains no double quote cannot escape its
 * quotes, whatever else it holds. {@link #quote} rejects the double quote outright rather than
 * doubling it, because doubling is easy to get subtly wrong and no legitimate column name needs it.
 * Control characters are rejected as well: they cannot help an attacker here, but they truncate or
 * corrupt identifiers in some drivers and always indicate a malformed payload.
 *
 * <p>Everything else stays legal, so identifiers that only work when quoted — spaces, mixed case,
 * reserved words, non-ASCII — keep working.
 */
public final class SqlIdentifiers {

  /**
   * PostgreSQL truncates identifiers at 63 bytes and most other engines cap in the same range. The
   * limit here is deliberately looser than any of them: its job is to stop absurd input early, not
   * to second-guess the target database.
   */
  public static final int MAX_LENGTH = 128;

  private SqlIdentifiers() {}

  /**
   * Validates {@code identifier} and returns it as a double-quoted SQL identifier.
   *
   * @param identifier the raw identifier
   * @param role what the identifier is, used in the error message (e.g. {@code "tableName"})
   * @throws IllegalArgumentException if the identifier is blank, too long, or contains a double
   *     quote or a control character
   */
  public static String quote(String identifier, String role) {
    if (StringUtils.isBlank(identifier)) {
      throw new IllegalArgumentException(role + " must not be blank");
    }
    if (identifier.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "%s is too long: %d characters, limit is %d", role, identifier.length(), MAX_LENGTH));
    }
    for (int i = 0; i < identifier.length(); i++) {
      char c = identifier.charAt(i);
      if (c == '"') {
        throw new IllegalArgumentException(
            String.format("%s must not contain a double quote: %s", role, identifier));
      }
      if (Character.isISOControl(c)) {
        throw new IllegalArgumentException(
            String.format("%s must not contain control characters: %s", role, identifier));
      }
    }
    return '"' + identifier + '"';
  }

  /**
   * Returns {@code "schema"."table"}, or just {@code "table"} when no schema is configured. Both
   * parts are validated by {@link #quote}.
   */
  public static String qualifiedTable(String schemaName, String tableName) {
    String table = quote(tableName, "tableName");
    if (StringUtils.isBlank(schemaName)) {
      return table;
    }
    return quote(schemaName, "schemaName") + "." + table;
  }
}
