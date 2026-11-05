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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import org.joni.Matcher;
import org.opensearch.grok.Grok;
import org.opensearch.grok.MatcherWatchdog;

@UtilityClass
public class GrokUtils {

  private static final long MATCH_TIMEOUT_MILLIS = 1_000;
  private static final long WATCHDOG_CHECK_INTERVAL_MILLIS = 100;
  private static final int EXPANSION_ATTEMPTS_TOLERATING_ONE_JVM_STALL = 2;

  // The built-in definition nests "+" inside "*" inside "+", which is exponential on a failed
  // match. This one matches the same text in the same backtracking order, but has no numbered
  // capture groups, is still quadratic in the number of slashes when searching, and is slower on
  // a path segment of several megabytes.
  private static final String UNIXPATH_WITHOUT_NESTED_QUANTIFIER =
      "(?:/(?:[\\w_%!$@:.,+~-]|\\\\.)*)+";

  private static final Map<String, String> PATTERN_BANK = buildPatternBank();

  private static final ScheduledExecutorService WATCHDOG_SCHEDULER =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().daemon().name("grok-watchdog").factory());

  private static final MatcherWatchdog MATCHER_WATCHDOG =
      MatcherWatchdog.newInstance(
          WATCHDOG_CHECK_INTERVAL_MILLIS,
          MATCH_TIMEOUT_MILLIS,
          () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()),
          (delay, command) -> WATCHDOG_SCHEDULER.schedule(command, delay, TimeUnit.MILLISECONDS));

  public static Grok compile(String grokPattern, Consumer<String> logCallback) {
    for (int attempt = 0; attempt < EXPANSION_ATTEMPTS_TOLERATING_ONE_JVM_STALL; attempt++) {
      var watchdog = new ExpansionCheckingWatchdog();
      Grok grok = new Grok(PATTERN_BANK, grokPattern, watchdog, logCallback);
      if (!watchdog.finishExpansion()) {
        return grok;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "grok expression could not be expanded within [%d] ms: %s",
            MATCH_TIMEOUT_MILLIS, grokPattern));
  }

  private static Map<String, String> buildPatternBank() {
    Map<String, String> bank = new HashMap<>(Grok.BUILTIN_PATTERNS);
    bank.put("UNIXPATH", UNIXPATH_WITHOUT_NESTED_QUANTIFIER);
    return Map.copyOf(bank);
  }

  /**
   * The Grok constructor expands %{...} references under the same watchdog as matching, and reads
   * an interrupted search as "no more references". That would compile a truncated expression
   * without any error, so this reports a search that ran past the limit during expansion.
   */
  private static final class ExpansionCheckingWatchdog implements MatcherWatchdog {
    private volatile boolean expanding = true;
    private long searchStartNanos;
    private boolean expansionTimedOut;

    @Override
    public void register(Matcher matcher) {
      if (expanding) {
        searchStartNanos = System.nanoTime();
      }
      MATCHER_WATCHDOG.register(matcher);
    }

    @Override
    public void unregister(Matcher matcher) {
      MATCHER_WATCHDOG.unregister(matcher);
      if (expanding
          && TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - searchStartNanos)
              >= MATCH_TIMEOUT_MILLIS) {
        expansionTimedOut = true;
      }
    }

    @Override
    public long maxExecutionTimeInMillis() {
      return MATCHER_WATCHDOG.maxExecutionTimeInMillis();
    }

    boolean finishExpansion() {
      expanding = false;
      return expansionTimedOut;
    }
  }
}
