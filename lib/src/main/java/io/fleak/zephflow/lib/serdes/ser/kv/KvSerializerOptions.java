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
package io.fleak.zephflow.lib.serdes.ser.kv;

import io.fleak.zephflow.lib.serdes.ser.FieldNames;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public record KvSerializerOptions(List<String> fields, String pairDelimiter, boolean cleanFields) {

  private static final Pattern FORBIDDEN_DELIMITER_CHARS =
      Pattern.compile("[A-Za-z0-9_.:/=\"\\\\\\r\\n-]");

  public static final String DEFAULT_PAIR_DELIMITER = " ";

  public static final KvSerializerOptions DEFAULT =
      new KvSerializerOptions(null, DEFAULT_PAIR_DELIMITER, false);

  public KvSerializerOptions {
    if (fields != null) {
      Set<String> names = new HashSet<>();
      for (String field : fields) {
        String name = FieldNames.normalize(field);
        if (StringUtils.isEmpty(name)) {
          throw new IllegalArgumentException("KV field names must not be blank");
        }
        if (!names.add(name)) {
          throw new IllegalArgumentException("Duplicate KV field: " + name);
        }
      }
      fields = List.copyOf(fields);
    }
    if (StringUtils.isEmpty(pairDelimiter)) {
      pairDelimiter = DEFAULT_PAIR_DELIMITER;
    }
    if (FORBIDDEN_DELIMITER_CHARS.matcher(pairDelimiter).find()) {
      throw new IllegalArgumentException(
          "KV pair delimiter must not contain letters, digits, '_', '.', ':', '/', '-', '=',"
              + " '\"', '\\' or line breaks");
    }
  }

  public static KvSerializerOptions of(
      List<String> fields, String pairDelimiter, Boolean cleanFields) {
    return new KvSerializerOptions(fields, pairDelimiter, Boolean.TRUE.equals(cleanFields));
  }

  public boolean hasPinnedFields() {
    return fields != null && !fields.isEmpty();
  }
}
