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
package io.fleak.zephflow.lib.sql.exec.types;

import io.fleak.zephflow.lib.sql.ast.types.LikeStringPattern;
import java.math.BigDecimal;

public abstract class BooleanLogic<T> {

  public <R> boolean greaterThan(T left, R right) {
    throw new RuntimeException("> is not supported for this type " + left.getClass());
  }

  public <R> boolean greaterThanOrEqual(T left, R right) {
    throw new RuntimeException(">= is not supported for this type " + left.getClass());
  }

  public <R> boolean lessThan(T left, R right) {
    throw new RuntimeException("< is not supported for this type " + left.getClass());
  }

  public <R> boolean lessThanOrEqual(T left, R right) {
    throw new RuntimeException("<= is not supported for this type " + left.getClass());
  }

  public <R> boolean equal(T left, R right) {
    throw new RuntimeException("= is not supported for this type " + left.getClass());
  }

  public <R> boolean notEqual(T left, R right) {
    return !equal(left, right);
  }

  public <R> boolean like(T left, R right) {
    throw new RuntimeException("like is not supported for this type " + left.getClass());
  }

  public <R> boolean ilike(T left, R right) {
    throw new RuntimeException("ilike is not supported for this type " + left.getClass());
  }

  public <R> boolean notLike(T left, R right) {
    return !like(left, right);
  }

  public <R> boolean notILike(T left, R right) {
    return !ilike(left, right);
  }

  @SuppressWarnings("unused")
  public <R> boolean in(T left, R right) {
    throw new RuntimeException("in like is not supported for this type " + left.getClass());
  }

  public <R> boolean notIn(T left, R right) {
    return !in(left, right);
  }

  public static class BoolBooleanLogic extends BooleanLogic<Boolean> {
    @Override
    public <R> boolean greaterThan(Boolean left, R right) {
      return Boolean.compare(left, ((Boolean) right)) > 0;
    }

    @Override
    public <R> boolean greaterThanOrEqual(Boolean left, R right) {
      return Boolean.compare(left, ((Boolean) right)) >= 0;
    }

    @Override
    public <R> boolean lessThan(Boolean left, R right) {
      return Boolean.compare(left, ((Boolean) right)) < 0;
    }

    @Override
    public <R> boolean lessThanOrEqual(Boolean left, R right) {
      return Boolean.compare(left, ((Boolean) right)) <= 0;
    }

    @Override
    public <R> boolean equal(Boolean left, R right) {
      return Boolean.compare(left, ((Boolean) right)) == 0;
    }
  }

  public static class LongBooleanLogic extends BooleanLogic<Number> {
    @Override
    public <R> boolean greaterThan(Number left, R right) {
      return left.longValue() > ((Number) right).longValue();
    }

    @Override
    public <R> boolean greaterThanOrEqual(Number left, R right) {
      return left.longValue() >= ((Number) right).longValue();
    }

    @Override
    public <R> boolean lessThan(Number left, R right) {
      return left.longValue() < ((Number) right).longValue();
    }

    @Override
    public <R> boolean lessThanOrEqual(Number left, R right) {
      return left.longValue() <= ((Number) right).longValue();
    }

    @Override
    public <R> boolean equal(Number left, R right) {
      return left.longValue() == ((Number) right).longValue();
    }
  }

  public static class DoubleBooleanLogic extends BooleanLogic<Number> {
    @Override
    public <R> boolean greaterThan(Number left, R right) {
      return left.doubleValue() > ((Number) right).doubleValue();
    }

    @Override
    public <R> boolean greaterThanOrEqual(Number left, R right) {
      return equal(left, right) || greaterThan(left, right);
    }

    @Override
    public <R> boolean lessThan(Number left, R right) {
      return left.doubleValue() < ((Number) right).doubleValue();
    }

    @Override
    public <R> boolean lessThanOrEqual(Number left, R right) {
      return equal(left, right) || lessThan(left, right);
    }

    @Override
    public <R> boolean equal(Number left, R right) {
      double ep = Math.abs(left.doubleValue() - ((Number) right).doubleValue());
      return ep < 1e-10;
    }
  }

  public static class BigDecimalBooleanLogic extends BooleanLogic<BigDecimal> {
    @Override
    public <R> boolean greaterThan(BigDecimal left, R right) {
      var i = left.compareTo((BigDecimal) right);
      return i > 0;
    }

    @Override
    public <R> boolean greaterThanOrEqual(BigDecimal left, R right) {
      var i = left.compareTo((BigDecimal) right);
      return i >= 0;
    }

    @Override
    public <R> boolean lessThan(BigDecimal left, R right) {
      var i = left.compareTo((BigDecimal) right);
      return i < 0;
    }

    @Override
    public <R> boolean lessThanOrEqual(BigDecimal left, R right) {
      var i = left.compareTo((BigDecimal) right);
      return i <= 0;
    }

    @Override
    public <R> boolean equal(BigDecimal left, R right) {
      var i = left.compareTo((BigDecimal) right);
      return i == 0;
    }
  }

  public static class StringBooleanLogic extends BooleanLogic<String> {
    @Override
    public <R> boolean equal(String left, R right) {
      return left.equals(right);
    }

    @Override
    public <R> boolean like(String left, R right) {
      return equal(left, right);
    }

    @Override
    public <R> boolean ilike(String left, R right) {
      return left.equalsIgnoreCase((String) right);
    }
  }

  public static class LikePatternBooleanLogic extends BooleanLogic<String> {
    @Override
    public <R> boolean like(String left, R right) {
      var matcher = (LikeStringPattern) right;
      return matcher.match(left);
    }

    @Override
    public <R> boolean ilike(String left, R right) {
      var matcher = (LikeStringPattern) right;
      return matcher.match(left);
    }
  }
}
