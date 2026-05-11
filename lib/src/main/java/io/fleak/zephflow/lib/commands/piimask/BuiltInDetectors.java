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

import java.util.regex.Pattern;

public enum BuiltInDetectors {
  EMAIL("email", "[EMAIL]", "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"),

  PHONE(
      "phone",
      "[PHONE]",
      // E.164 international first (so a leading + is consumed as a whole), then US.
      // Order matters: regex alternation is left-to-right; without the E.164-first
      // ordering, the US alternative would match the trailing digits of an E.164
      // number and leave a stray "+" in the output.
      "\\+[2-9]\\d{1,14}" + "|(?:\\+?1[-.\\s]?)?\\(?\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4}"),

  SSN(
      "ssn",
      "[SSN]",
      // dashed OR no-dash with area/group/serial validity (no 000/666 area, no 00 group, no 0000
      // serial)
      "\\b\\d{3}-\\d{2}-\\d{4}\\b" + "|\\b(?!000|666)[0-8]\\d{2}(?!00)\\d{2}(?!0000)\\d{4}\\b"),

  CREDIT_CARD("creditCard", "[CC]", "\\b(?:\\d[ -]*?){13,19}\\b"),

  IPV4("ipv4", "[IPV4]", "\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b"),

  IPV6(
      "ipv6",
      "[IPV6]",
      // catches full and ::-compressed forms; requires at least 2 colons to avoid `aa:bb`
      "(?:[A-Fa-f0-9]{1,4}:){2,7}[A-Fa-f0-9]{1,4}" + "|(?:[A-Fa-f0-9]{1,4}:){1,7}:");

  private final String configKey;
  private final String defaultToken;
  private final Pattern pattern;

  BuiltInDetectors(String configKey, String defaultToken, String regex) {
    this.configKey = configKey;
    this.defaultToken = defaultToken;
    this.pattern = Pattern.compile(regex);
  }

  public String configKey() {
    return configKey;
  }

  public String defaultToken() {
    return defaultToken;
  }

  public Pattern pattern() {
    return pattern;
  }
}
