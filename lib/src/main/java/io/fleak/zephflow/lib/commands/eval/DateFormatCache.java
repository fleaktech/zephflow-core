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
package io.fleak.zephflow.lib.commands.eval;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * Thread-local cache for SimpleDateFormat instances. SimpleDateFormat is not thread-safe, so we use
 * ThreadLocal to ensure each thread has its own cache. The inner map caches by pattern string +
 * timezone key.
 */
final class DateFormatCache {

  private DateFormatCache() {}

  private static final ThreadLocal<Map<String, SimpleDateFormat>> CACHE =
      ThreadLocal.withInitial(HashMap::new);

  /**
   * Get or create a cached SimpleDateFormat for the given pattern and timezone.
   *
   * @param pattern the date pattern
   * @param timeZone the timezone (null for default)
   * @return a cached SimpleDateFormat instance
   */
  static SimpleDateFormat getCachedDateFormat(String pattern, TimeZone timeZone) {
    String cacheKey = pattern + "|" + (timeZone != null ? timeZone.getID() : "default");
    return CACHE
        .get()
        .computeIfAbsent(
            cacheKey,
            k -> {
              SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
              if (timeZone != null) {
                sdf.setTimeZone(timeZone);
              }
              return sdf;
            });
  }
}
