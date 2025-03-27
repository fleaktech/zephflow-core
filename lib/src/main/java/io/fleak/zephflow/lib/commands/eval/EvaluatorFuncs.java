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

import com.google.common.collect.ImmutableMap;
import io.fleak.zephflow.api.structure.*;
import java.util.Map;
import java.util.Objects;

/** Created by bolei on 10/19/24 */
public interface EvaluatorFuncs {
  interface UnaryEvaluatorFunc<T> {
    T evaluate(T operand);
  }

  interface BinaryEvaluatorFunc<T> {
    T evaluate(T left, T right);
  }

  /****************** Unary Value ******************/
  abstract class ValueUnaryEvaluator implements UnaryEvaluatorFunc<FleakData> {
    @Override
    public FleakData evaluate(FleakData operand) {
      if (operand == null) {
        return null;
      }
      if (!validateType(operand)) {
        return null;
      }
      return doEvaluate(operand);
    }

    protected abstract FleakData doEvaluate(FleakData o);

    protected abstract boolean validateType(FleakData o);
  }

  abstract class ValueBinaryEvaluator implements BinaryEvaluatorFunc<FleakData> {
    @Override
    public FleakData evaluate(FleakData left, FleakData right) {
      if (left == null || right == null) {
        return null;
      }
      if (!validateTypes(left, right)) {
        throw new IllegalArgumentException(
            String.format(
                "unsupported operand types for + operator: left=%s, right=%s",
                left.unwrap(), right.unwrap()));
      }
      return doEvaluate(left, right);
    }

    protected abstract boolean validateTypes(FleakData l, FleakData r);

    abstract FleakData doEvaluate(FleakData l, FleakData r);
  }

  ValueUnaryEvaluator NOT_VALUE_EVALUATOR_FUNC =
      new ValueUnaryEvaluator() {
        @Override
        protected FleakData doEvaluate(FleakData o) {
          return new BooleanPrimitiveFleakData(!o.isTrueValue());
        }

        @Override
        protected boolean validateType(FleakData o) {
          return o instanceof BooleanPrimitiveFleakData;
        }
      };

  ValueUnaryEvaluator NEGATE_VALUE_EVALUATOR_FUNC =
      new ValueUnaryEvaluator() {
        @Override
        protected FleakData doEvaluate(FleakData o) {
          return new NumberPrimitiveFleakData(o.getNumberValue() * -1, o.getNumberType());
        }

        @Override
        protected boolean validateType(FleakData o) {
          return o instanceof NumberPrimitiveFleakData;
        }
      };
  Map<String, UnaryEvaluatorFunc<FleakData>> UNARY_VALUE_EVALUATOR_FUNC_MAP =
      new ImmutableMap.Builder<String, UnaryEvaluatorFunc<FleakData>>()
          .put("-", NEGATE_VALUE_EVALUATOR_FUNC)
          .put("not", NOT_VALUE_EVALUATOR_FUNC)
          .build();

  /****************** Binary Value ******************/
  // Binary Arithmetic Operators
  abstract class NumericValueBinaryEvaluator extends ValueBinaryEvaluator {
    @Override
    protected boolean validateTypes(FleakData left, FleakData right) {
      return left instanceof NumberPrimitiveFleakData && right instanceof NumberPrimitiveFleakData;
    }
  }

  ValueBinaryEvaluator PLUS_VALUE_EVALUATOR_FUNC =
      new ValueBinaryEvaluator() {
        @Override
        protected boolean validateTypes(FleakData l, FleakData r) {
          return true;
        }

        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          // str + str
          if (l instanceof StringPrimitiveFleakData && r instanceof StringPrimitiveFleakData) {
            return new StringPrimitiveFleakData(l.getStringValue() + r.getStringValue());
          }
          // num + num
          if (l instanceof NumberPrimitiveFleakData && r instanceof NumberPrimitiveFleakData) {
            double sum = l.getNumberValue() + r.getNumberValue();
            return new NumberPrimitiveFleakData(sum, NumberPrimitiveFleakData.NumberType.DOUBLE);
          }
          throw new IllegalArgumentException(
              String.format(
                  "unsupported operand types for + operator: left=%s, right=%s",
                  l.unwrap(), r.unwrap()));
        }
      };

  NumericValueBinaryEvaluator MINUS_VALUE_EVALUATOR_FUNC =
      new NumericValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          double minus = l.getNumberValue() - r.getNumberValue();
          return new NumberPrimitiveFleakData(minus, NumberPrimitiveFleakData.NumberType.DOUBLE);
        }
      };
  NumericValueBinaryEvaluator TIMES_VALUE_EVALUATOR_FUNC =
      new NumericValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          double timesResult = l.getNumberValue() * r.getNumberValue();
          return new NumberPrimitiveFleakData(
              timesResult, NumberPrimitiveFleakData.NumberType.DOUBLE);
        }
      };
  NumericValueBinaryEvaluator DIVIDE_VALUE_EVALUATOR_FUNC =
      new NumericValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          double dividend = l.getNumberValue();
          double divisor = r.getNumberValue();
          if (divisor == 0) {
            return null;
          }
          double quotient = dividend / divisor;
          return new NumberPrimitiveFleakData(quotient, NumberPrimitiveFleakData.NumberType.DOUBLE);
        }
      };
  NumericValueBinaryEvaluator MOD_VALUE_EVALUATOR_FUNC =
      new NumericValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          if (r.getNumberValue() == 0) {
            return null;
          }
          double modResult = l.getNumberValue() % r.getNumberValue();
          return new NumberPrimitiveFleakData(
              modResult, NumberPrimitiveFleakData.NumberType.DOUBLE);
        }
      };

  // Binary Comparison Operators
  abstract class ComparisonValueBinaryEvaluator extends ValueBinaryEvaluator {
    @Override
    protected boolean validateTypes(FleakData l, FleakData r) {
      return FleakData.valueComparable(l, r);
    }
  }

  ComparisonValueBinaryEvaluator LESS_THAN_OR_EQUAL_VALUE_EVALUATOR_FUNC =
      new ComparisonValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          return new BooleanPrimitiveFleakData(l.compareTo(r) <= 0);
        }
      };
  ComparisonValueBinaryEvaluator LESS_THAN_VALUE_EVALUATOR_FUNC =
      new ComparisonValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          return new BooleanPrimitiveFleakData(l.compareTo(r) < 0);
        }
      };
  ComparisonValueBinaryEvaluator GREATER_THAN_OR_EQUAL_VALUE_EVALUATOR_FUNC =
      new ComparisonValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          return new BooleanPrimitiveFleakData(l.compareTo(r) >= 0);
        }
      };
  ComparisonValueBinaryEvaluator GREATER_THAN_VALUE_EVALUATOR_FUNC =
      new ComparisonValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          return new BooleanPrimitiveFleakData(l.compareTo(r) > 0);
        }
      };

  ComparisonValueBinaryEvaluator EQUALS_VALUE_EVALUATOR_FUNC =
      new ComparisonValueBinaryEvaluator() {
        @Override
        public FleakData evaluate(FleakData left, FleakData right) {
          return new BooleanPrimitiveFleakData(Objects.equals(left, right));
        }

        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          throw new UnsupportedOperationException();
        }
      };
  ComparisonValueBinaryEvaluator NOT_EQUALS_VALUE_EVALUATOR_FUNC =
      new ComparisonValueBinaryEvaluator() {
        @Override
        public FleakData evaluate(FleakData left, FleakData right) {
          return new BooleanPrimitiveFleakData(!Objects.equals(left, right));
        }

        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          throw new UnsupportedOperationException();
        }
      };

  // Binary boolean operator
  abstract class BooleanValueBinaryEvaluator extends ValueBinaryEvaluator {
    @Override
    protected boolean validateTypes(FleakData l, FleakData r) {
      return l instanceof BooleanPrimitiveFleakData && r instanceof BooleanPrimitiveFleakData;
    }
  }

  BooleanValueBinaryEvaluator AND_VALUE_EVALUATOR_FUNC =
      new BooleanValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          return new BooleanPrimitiveFleakData(l.isTrueValue() && r.isTrueValue());
        }
      };
  BooleanValueBinaryEvaluator OR_VALUE_EVALUATOR_FUNC =
      new BooleanValueBinaryEvaluator() {
        @Override
        FleakData doEvaluate(FleakData l, FleakData r) {
          return new BooleanPrimitiveFleakData(l.isTrueValue() || r.isTrueValue());
        }
      };

  Map<String, BinaryEvaluatorFunc<FleakData>> BINARY_VALUE_EVALUATOR_FUNC_MAP =
      new ImmutableMap.Builder<String, BinaryEvaluatorFunc<FleakData>>()
          .put("+", PLUS_VALUE_EVALUATOR_FUNC)
          .put("-", MINUS_VALUE_EVALUATOR_FUNC)
          .put("*", TIMES_VALUE_EVALUATOR_FUNC)
          .put("/", DIVIDE_VALUE_EVALUATOR_FUNC)
          .put("%", MOD_VALUE_EVALUATOR_FUNC)
          .put("<", LESS_THAN_VALUE_EVALUATOR_FUNC)
          .put(">", GREATER_THAN_VALUE_EVALUATOR_FUNC)
          .put("<=", LESS_THAN_OR_EQUAL_VALUE_EVALUATOR_FUNC)
          .put(">=", GREATER_THAN_OR_EQUAL_VALUE_EVALUATOR_FUNC)
          .put("==", EQUALS_VALUE_EVALUATOR_FUNC)
          .put("!=", NOT_EQUALS_VALUE_EVALUATOR_FUNC)
          .put("and", AND_VALUE_EVALUATOR_FUNC)
          .put("or", OR_VALUE_EVALUATOR_FUNC)
          .build();
}
