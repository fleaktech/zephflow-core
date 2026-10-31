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

import java.util.Arrays;

public enum DetectedFormat {
  JSON_OBJECT("json_object", "JSON Object"),
  JSON_ARRAY("json_array", "JSON Array"),
  SYSLOG_CEF("syslog_cef", "Syslog (CEF)"),
  SYSLOG_PLAIN("syslog_plain", "Syslog (Plain Text)"),
  YAML("yaml", "YAML"),
  CSV("csv", "CSV"),
  UNRECOGNIZED_TEXT("unrecognized_text", "Unrecognized text");

  private final String id;
  private final String label;

  DetectedFormat(String id, String label) {
    this.id = id;
    this.label = label;
  }

  public String id() {
    return id;
  }

  public String label() {
    return label;
  }

  public static DetectedFormat fromId(String id) {
    return Arrays.stream(values())
        .filter(value -> value.id.equals(id))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown format: " + id));
  }
}
