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

import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FieldPopulationSelector;
import io.fleak.zephflow.api.structure.FieldPopulationSelector.PathSegment;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared Pattern classification semantics for inspection and compiled FEEL filters. */
public final class FieldPatternMatcher {
  public static final String CONTRACT_VERSION = "field-pattern-v1";
  public static final String EVALUATION_LIMIT = "PATTERN_EVALUATION_LIMIT";
  public static final String STACK_LIMIT = "PATTERN_STACK_LIMIT";
  public static final long CHARACTER_BUDGET = 5_000_000L;

  private FieldPatternMatcher() {}

  /** Compiles the exact Java regex; syntax exceptions retain their description and index. */
  public static Pattern compile(String expression) {
    Objects.requireNonNull(expression, "Pattern is required");
    checkInterrupted();
    try {
      return Pattern.compile(expression);
    } catch (StackOverflowError error) {
      throw new IllegalArgumentException(
          STACK_LIMIT + ": regex compilation exceeded the stack", error);
    }
  }

  /** Presence includes null and containers; matched requires at least one scalar match. */
  public record Result(boolean present, boolean matched) {}

  /** Group-zero match in one physical scalar; offsets are UTF-16, with an exclusive end. */
  public record MatchSpan(List<PathSegment> location, int from, int to) {
    public MatchSpan {
      location = List.copyOf(location);
      if (from < 0 || to < from) throw new IllegalArgumentException("Invalid match range");
    }
  }

  /** Evaluates a compiled filter without allocating match spans. */
  public static Result evaluate(FleakData record, String fieldName, Pattern pattern) {
    return evaluate(record, fieldName, pattern, () -> {}, null);
  }

  /**
   * Evaluates every selected scalar and every successive find, including after the first match. The
   * optional span consumer is used only when rendering a page. The activity check must be
   * inexpensive: it is called before finds and on character reads, in addition to thread checks.
   */
  public static Result evaluate(
      FleakData record,
      String fieldName,
      Pattern pattern,
      Runnable checkActive,
      Consumer<MatchSpan> spanConsumer) {
    Objects.requireNonNull(pattern);
    Objects.requireNonNull(checkActive);
    checkInterrupted();
    checkActive.run();
    boolean[] matched = {false};
    boolean present =
        FieldPopulationSelector.visitScalars(
            record,
            fieldName,
            (location, value) -> {
              checkInterrupted();
              checkActive.run();
              Optional<String> text = scalarText(value);
              if (text.isEmpty()) return;
              Budget budget = new Budget(CHARACTER_BUDGET, checkActive);
              Matcher matcher =
                  pattern.matcher(new BudgetedText(text.get(), 0, text.get().length(), budget));
              while (true) {
                budget.checkActive();
                boolean found;
                try {
                  found = matcher.find();
                } catch (StackOverflowError error) {
                  throw new IllegalArgumentException(
                      STACK_LIMIT + ": regex evaluation exceeded the stack", error);
                }
                if (!found) break;
                matched[0] = true;
                if (spanConsumer != null) {
                  spanConsumer.accept(new MatchSpan(location, matcher.start(), matcher.end()));
                }
              }
            });
    return new Result(present, matched[0]);
  }

  /** Exact scalar text shared by the matcher and the inspection renderer. */
  public static Optional<String> scalarText(FleakData value) {
    if (value instanceof StringPrimitiveFleakData string) {
      return Optional.of(string.getStringValue());
    }
    if (value instanceof NumberPrimitiveFleakData || value instanceof BooleanPrimitiveFleakData) {
      return Optional.of(String.valueOf(value.unwrap()));
    }
    return Optional.empty();
  }

  private static void checkInterrupted() {
    if (Thread.currentThread().isInterrupted()) {
      throw new CancellationException("Pattern evaluation was interrupted");
    }
  }

  static final class Budget {
    private long remaining;
    private final Runnable activity;

    Budget(long remaining, Runnable activity) {
      this.remaining = remaining;
      this.activity = activity;
    }

    private void checkActive() {
      checkInterrupted();
      activity.run();
    }

    private void read() {
      checkActive();
      if (--remaining < 0) {
        throw new IllegalArgumentException(
            EVALUATION_LIMIT + ": regex exceeded the character-read budget for one scalar");
      }
    }
  }

  static final class BudgetedText implements CharSequence {
    private final String text;
    private final int start;
    private final int end;
    private final Budget budget;

    BudgetedText(String text, int start, int end, Budget budget) {
      this.text = text;
      this.start = start;
      this.end = end;
      this.budget = budget;
    }

    @Override
    public int length() {
      return end - start;
    }

    @Override
    public char charAt(int index) {
      Objects.checkIndex(index, length());
      budget.read();
      return text.charAt(start + index);
    }

    @Override
    public CharSequence subSequence(int from, int to) {
      Objects.checkFromToIndex(from, to, length());
      return new BudgetedText(text, start + from, start + to, budget);
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder(length());
      for (int index = 0; index < length(); index++) result.append(charAt(index));
      return result.toString();
    }
  }
}
