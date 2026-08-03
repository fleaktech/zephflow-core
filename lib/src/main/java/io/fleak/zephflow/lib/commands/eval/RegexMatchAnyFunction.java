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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/*
regexMatchAnyFunction:
Full-match a regular expression against a value that may be a scalar OR an array. For an array it
returns true if any element (by its string form) fully matches. Non-string scalars are matched by
their string form; records and nulls never match.

Syntax:
regex_match_any($.path.to.field, "<regex_pattern>")
*/
class RegexMatchAnyFunction implements FeelFunction {
  private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "regex_match_any", 2, "value (scalar or array) and regex pattern");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData patternFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "regex_match_any: pattern must be a string: %s",
        patternFd);
    Pattern pattern = patternCache.computeIfAbsent(patternFd.getStringValue(), Pattern::compile);
    return new BooleanPrimitiveFleakData(matches(evaluatedArgs.getFirst(), pattern, true));
  }

  /**
   * Cap on character accesses per match. A pathological (catastrophic-backtracking) pattern blows
   * past this quickly; we then treat it as "no match" rather than letting it hang the pipeline.
   */
  static final long MATCH_BUDGET = 5_000_000L;

  /** True if {@code value} (or any element, if it is an array) matches the pattern. */
  static boolean matches(FleakData value, Pattern pattern, boolean fullMatch) {
    if (value == null) {
      return false;
    }
    if (value instanceof ArrayFleakData array) {
      for (FleakData element : array.getArrayPayload()) {
        if (matches(element, pattern, fullMatch)) {
          return true;
        }
      }
      return false;
    }
    if (value instanceof RecordFleakData) {
      return false;
    }
    Object raw = value.unwrap();
    if (raw == null) {
      return false;
    }
    BudgetedCharSequence input = new BudgetedCharSequence(String.valueOf(raw), MATCH_BUDGET);
    try {
      return fullMatch ? pattern.matcher(input).matches() : pattern.matcher(input).find();
    } catch (BudgetExceededException e) {
      return false; // over the match budget -> treat as no match, per the design
    }
  }

  private static final class BudgetExceededException extends RuntimeException {
    BudgetExceededException() {
      super(null, null, false, false);
    }
  }

  /**
   * A CharSequence that aborts the regex engine once it has read more than {@code budget} chars.
   */
  private static final class BudgetedCharSequence implements CharSequence {
    private final CharSequence delegate;
    private long remaining;

    BudgetedCharSequence(CharSequence delegate, long budget) {
      this.delegate = delegate;
      this.remaining = budget;
    }

    @Override
    public char charAt(int index) {
      if (--remaining < 0) {
        throw new BudgetExceededException();
      }
      return delegate.charAt(index);
    }

    @Override
    public int length() {
      return delegate.length();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return delegate.subSequence(start, end);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}
