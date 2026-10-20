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
package io.fleak.zephflow.lib.serdes.ser;

import java.util.regex.Pattern;

public final class FieldNames {

  private static final Pattern FORMAT_CHARS = Pattern.compile("\\p{Cf}");
  private static final Pattern EDGE_WHITESPACE = Pattern.compile("^[\\s\\p{Z}]+|[\\s\\p{Z}]+$");

  private FieldNames() {}

  public static String normalize(String name) {
    if (name == null) {
      return "";
    }
    String visible = FORMAT_CHARS.matcher(name).replaceAll("");
    return EDGE_WHITESPACE.matcher(visible).replaceAll("");
  }
}
