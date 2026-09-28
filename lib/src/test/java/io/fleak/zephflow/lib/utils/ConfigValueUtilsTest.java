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

import static io.fleak.zephflow.lib.utils.ConfigValueUtils.checkNoUnknownKeys;
import static io.fleak.zephflow.lib.utils.ConfigValueUtils.requireInteger;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConfigValueUtilsTest {

  static Stream<Arguments> acceptedIntegers() {
    return Stream.of(
        Arguments.of(0, 0L),
        Arguments.of(10, 10L),
        Arguments.of(5L, 5L),
        Arguments.of((short) 7, 7L),
        Arguments.of((byte) 3, 3L),
        Arguments.of(BigInteger.TEN, 10L));
  }

  @ParameterizedTest
  @MethodSource("acceptedIntegers")
  void requireInteger_acceptsIntegralTypesWithinInclusiveRange(Object value, long expected) {
    assertEquals(expected, requireInteger(value, "p", 0, 10));
  }

  static Stream<Arguments> rejectedValues() {
    return Stream.of(
        Arguments.of(null, "'p' must be an integer, got: null"),
        Arguments.of("3", "'p' must be an integer, got: \"3\""),
        Arguments.of(3.0, "'p' must be an integer, got: 3.0"),
        Arguments.of(new BigDecimal("3.5"), "'p' must be an integer, got: 3.5"),
        Arguments.of(true, "'p' must be an integer, got: true"),
        Arguments.of(List.of(3), "'p' must be an integer, got: [3]"),
        Arguments.of(-1, "'p' must be between 0 and 10, got: -1"),
        Arguments.of(11L, "'p' must be between 0 and 10, got: 11"),
        Arguments.of(
            new BigInteger("99999999999999999999"),
            "'p' must be between 0 and 10, got: 99999999999999999999"));
  }

  @ParameterizedTest
  @MethodSource("rejectedValues")
  void requireInteger_rejectsWithClearMessage(Object value, String expectedMessage) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> requireInteger(value, "p", 0, 10));
    assertEquals(expectedMessage, e.getMessage());
  }

  @Test
  void checkNoUnknownKeys_acceptsAllowedKeys() {
    assertDoesNotThrow(() -> checkNoUnknownKeys(Map.of("a", 1), Set.of("a", "b"), ""));
  }

  @Test
  void checkNoUnknownKeys_rejectsUnknownKeyWithPrefix() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> checkNoUnknownKeys(Map.of("c", 1), new TreeSet<>(Set.of("a", "b")), "rules[0]."));
    assertEquals("unknown parameter 'rules[0].c'; allowed: [a, b]", e.getMessage());
  }

  @Test
  void checkNoUnknownKeys_rejectsNonStringKey() {
    Map<Object, Object> map = new HashMap<>(Map.of(1, "x"));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> checkNoUnknownKeys(map, new TreeSet<>(Set.of("a")), ""));
    assertEquals("unknown parameter '1'; allowed: [a]", e.getMessage());
  }
}
