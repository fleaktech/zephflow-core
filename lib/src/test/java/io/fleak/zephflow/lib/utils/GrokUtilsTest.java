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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.grok.Grok;

class GrokUtilsTest {

  private static final Duration TEST_DEADLINE = Duration.ofSeconds(20);

  @Test
  void catastrophicBacktrackingIsInterrupted() {
    var grok =
        GrokUtils.compile(
            "%{GREEDYDATA:a} %{GREEDYDATA:b} %{GREEDYDATA:c} %{GREEDYDATA:d} %{GREEDYDATA:e} END",
            s -> {});
    var input =
        IntStream.range(0, 500).mapToObj(i -> "word" + i % 10).collect(Collectors.joining(" "));

    var thrown =
        assertTimeoutPreemptively(
            TEST_DEADLINE, () -> assertThrows(RuntimeException.class, () -> grok.captures(input)));

    assertEquals("grok pattern matching was interrupted after [1000] ms", thrown.getMessage());
  }

  @Test
  void unixPathNoMatchOnLongPathReturnsQuickly() {
    var grok = GrokUtils.compile("%{UNIXPATH:path} END", s -> {});
    var input = "/" + "a".repeat(4096) + " x";

    assertNull(assertTimeoutPreemptively(TEST_DEADLINE, () -> grok.captures(input)));
  }

  @Test
  void pathNoMatchOnLongPathReturnsQuickly() {
    var grok = GrokUtils.compile("%{PATH:path} END", s -> {});
    // %{PATH} stays quadratic on a failed search, so a much longer input can reach the match limit
    // on a cold JVM. The library's UNIXPATH is exponential well before this length.
    var input = "/" + "a".repeat(64) + " x END";

    assertNull(assertTimeoutPreemptively(TEST_DEADLINE, () -> grok.captures(input)));
  }

  @Test
  void unixPathOnLongPathCaptures() {
    var grok = GrokUtils.compile("%{UNIXPATH:path} END", s -> {});
    var path = "/" + "a".repeat(4096);

    assertEquals(Map.of("path", path), grok.captures(path + " END"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/var/log/app.log END",
        "/a/b\\ c/d END",
        "x /usr/local/bin/ END",
        "/ END",
        "//a//b END",
        "/a\\.b END",
        "/a\\/b END",
        "/a\\",
        "/trailing\\ END",
        "/a/b c END",
        "/a END /b END",
        "/\\\\ END",
        "no path END",
        "/ünï/cödé END"
      })
  void unixPathCapturesMatchBuiltinDefinition(String input) {
    var builtin = new Grok(Grok.BUILTIN_PATTERNS, "%{UNIXPATH:path} END", s -> {});
    var actual = GrokUtils.compile("%{UNIXPATH:path} END", s -> {});

    assertEquals(builtin.captures(input), actual.captures(input));
  }

  @Test
  void expansionThatRunsPastTheLimitIsRejected() {
    var pattern = "%{FOO=" + "a".repeat(200) + "{ %{WORD:x}";

    var thrown =
        assertTimeoutPreemptively(
            TEST_DEADLINE,
            () ->
                assertThrows(
                    IllegalArgumentException.class, () -> GrokUtils.compile(pattern, s -> {})));

    assertEquals(
        "grok expression could not be expanded within [1000] ms: " + pattern, thrown.getMessage());
  }
}
