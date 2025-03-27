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
package io.fleak.zephflow.lib.sql.ast.types;

import java.util.regex.Pattern;

public class LikeStringPattern {

  private final Pattern pattern;

  public LikeStringPattern(String like, boolean caseSensitive) {
    // @TODO we need to revisit this logic and test
    String regex = like.replaceAll("%", ".*").replaceAll("_", ".");

    // Add anchors to match the whole string
    regex = "^" + regex + "$";

    if (caseSensitive) this.pattern = Pattern.compile(regex);
    else this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
  }

  public static boolean isMatchString(String str) {
    return str.contains("%") || str.contains("_");
  }

  public boolean match(String value) {
    return pattern.matcher(value).matches();
  }
}
