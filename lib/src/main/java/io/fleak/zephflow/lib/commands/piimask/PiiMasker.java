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

import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class PiiMasker {

  private PiiMasker() {}

  public record Spec(
      String name, Pattern pattern, String replacement, Predicate<String> postFilter) {
    public boolean accept(String matchText) {
      return postFilter == null || postFilter.test(matchText);
    }
  }

  public static Spec builtIn(BuiltInDetectors d, String customReplacement) {
    String replacement = customReplacement != null ? customReplacement : d.defaultToken();
    Predicate<String> postFilter =
        switch (d) {
          case CREDIT_CARD -> PiiMasker::luhnValid;
          case IPV4 -> PiiMasker::ipv4OctetsValid;
          default -> null;
        };
    return new Spec(d.configKey(), d.pattern(), replacement, postFilter);
  }

  public static Spec custom(String name, Pattern pattern, String replacement) {
    return new Spec(name, pattern, replacement, null);
  }

  public static PiiPathWriter.RewriteResult mask(String input, List<Spec> specs) {
    String current = input;
    int total = 0;
    for (Spec spec : specs) {
      Matcher matcher = spec.pattern().matcher(current);
      StringBuilder sb = new StringBuilder();
      int count = 0;
      while (matcher.find()) {
        if (spec.accept(matcher.group())) {
          matcher.appendReplacement(sb, Matcher.quoteReplacement(spec.replacement()));
          count++;
        } else {
          matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group()));
        }
      }
      matcher.appendTail(sb);
      current = sb.toString();
      total += count;
    }
    return new PiiPathWriter.RewriteResult(current, total);
  }

  public static boolean luhnValid(String matchText) {
    String digits = matchText.replaceAll("[\\s-]", "");
    if (digits.length() < 13 || digits.length() > 19) {
      return false;
    }
    int sum = 0;
    boolean alt = false;
    for (int i = digits.length() - 1; i >= 0; i--) {
      char c = digits.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
      int n = c - '0';
      if (alt) {
        n *= 2;
        if (n > 9) n -= 9;
      }
      sum += n;
      alt = !alt;
    }
    return sum > 0 && sum % 10 == 0;
  }

  public static boolean ipv4OctetsValid(String matchText) {
    String[] parts = matchText.split("\\.");
    if (parts.length != 4) {
      return false;
    }
    for (String part : parts) {
      try {
        int v = Integer.parseInt(part);
        if (v < 0 || v > 255) return false;
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return true;
  }
}
