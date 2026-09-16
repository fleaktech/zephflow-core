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

public final class SqlIdentifiers {

  public static final int MAX_LENGTH = 128;

  private SqlIdentifiers() {}

  public static String quote(String identifier, String identifierRole) {
    if (StringUtils.isBlank(identifier)) {
      throw new IllegalArgumentException(identifierRole + " must not be blank");
    }
    if (identifier.length() > MAX_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "%s is too long: %d characters, limit is %d",
              identifierRole, identifier.length(), MAX_LENGTH));
    }
    for (int index = 0; index < identifier.length(); index++) {
      char character = identifier.charAt(index);
      if (character == '"') {
        throw new IllegalArgumentException(
            String.format("%s must not contain a double quote: %s", identifierRole, identifier));
      }
      if (Character.isISOControl(character)) {
        throw new IllegalArgumentException(
            String.format(
                "%s must not contain control characters: %s", identifierRole, identifier));
      }
    }
    return '"' + identifier + '"';
  }

  public static String qualifiedTable(String schemaName, String tableName) {
    String quotedTable = quote(tableName, "tableName");
    if (StringUtils.isBlank(schemaName)) {
      return quotedTable;
    }
    return quote(schemaName, "schemaName") + "." + quotedTable;
  }
}
