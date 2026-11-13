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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public interface ConfigValueUtils {

  /**
   * Applies the existing integer conversion and attaches the original input location on failure.
   */
  static long requireInteger(
      Object value, String name, long min, long max, ConfigurationPath path) {
    try {
      return requireInteger(value, name, min, max);
    } catch (IllegalArgumentException failure) {
      throw ConfigurationValidationException.at(
          path, failure, "Expected an integer from " + min + " to " + max);
    }
  }

  static long requireInteger(Object value, String name, long min, long max) {
    BigInteger n =
        switch (value) {
          case Integer i -> BigInteger.valueOf(i);
          case Long l -> BigInteger.valueOf(l);
          case Short s -> BigInteger.valueOf(s);
          case Byte b -> BigInteger.valueOf(b);
          case BigInteger b -> b;
          case null, default ->
              throw new IllegalArgumentException(
                  String.format(
                      "'%s' must be an integer, got: %s",
                      name, value instanceof String str ? "\"" + str + "\"" : value));
        };
    Preconditions.checkArgument(
        n.compareTo(BigInteger.valueOf(min)) >= 0 && n.compareTo(BigInteger.valueOf(max)) <= 0,
        "'%s' must be between %s and %s, got: %s",
        name,
        min,
        max,
        n);
    return n.longValueExact();
  }

  static void checkNoUnknownKeys(Map<?, ?> map, Set<String> allowed, String prefix) {
    checkNoUnknownKeys(map, allowed, prefix, null);
  }

  /** Attaches a literal key rather than parsing the legacy human-readable prefix. */
  static void checkNoUnknownKeys(
      Map<?, ?> map, Set<String> allowed, String prefix, ConfigurationPath path) {
    for (Object key : map.keySet()) {
      try {
        Preconditions.checkArgument(
            key instanceof String name && allowed.contains(name),
            "unknown parameter '%s%s'; allowed: %s",
            prefix,
            key,
            allowed);
      } catch (IllegalArgumentException failure) {
        if (path == null) throw failure;
        throw ConfigurationValidationException.at(
            key instanceof String name ? path.field(name) : path,
            failure,
            "Unknown parameter. Accepted fields: " + new TreeSet<>(allowed));
      }
    }
  }
}
