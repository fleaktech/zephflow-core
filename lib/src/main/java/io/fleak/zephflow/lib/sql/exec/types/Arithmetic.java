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

import java.math.BigDecimal;
import java.math.RoundingMode;

public abstract class Arithmetic<T> {

  public <R> T add(T left, R right) {
    throw new RuntimeException("addition is not supported for this type " + left.getClass());
  }

  public <R> T sub(T left, R right) {
    throw new RuntimeException("subtraction is not supported for this type " + left.getClass());
  }

  public <R> T mul(T left, R right) {
    throw new RuntimeException("multiplication is not supported for this type " + left.getClass());
  }

  public <R> T div(T left, R right) {
    throw new RuntimeException("division is not supported for this type " + left.getClass());
  }

  public <R> T mod(T left, R right) {
    throw new RuntimeException("modulus is not supported for this type " + left.getClass());
  }

  public <R> T power(T left, R right) {
    throw new RuntimeException("exponentiation is not supported for this type " + left.getClass());
  }

  public <R> T concat(T left, R right) {
    throw new RuntimeException("concatenation is not supported for this type " + left.getClass());
  }

  public static class LongArithmetic extends Arithmetic<Number> {
    @Override
    public <R> Number add(Number left, R right) {
      return left.longValue() + ((Number) right).longValue();
    }

    @Override
    public <R> Number sub(Number left, R right) {
      return left.longValue() - ((Number) right).longValue();
    }

    @Override
    public <R> Number mul(Number left, R right) {
      return left.longValue() * ((Number) right).longValue();
    }

    @Override
    public <R> Number div(Number left, R right) {
      return left.longValue() / ((Number) right).longValue();
    }

    @Override
    public <R> Number mod(Number left, R right) {
      return left.longValue() % ((Number) right).longValue();
    }

    @Override
    public <R> Number power(Number left, R right) {
      return Double.valueOf(Math.pow(left.doubleValue(), ((Number) right).doubleValue()))
          .longValue();
    }
  }

  public static class DoubleArithmetic extends Arithmetic<Number> {
    @Override
    public <R> Number add(Number left, R right) {
      return left.doubleValue() + ((Number) right).doubleValue();
    }

    @Override
    public <R> Number sub(Number left, R right) {
      return left.doubleValue() - ((Number) right).doubleValue();
    }

    @Override
    public <R> Number mul(Number left, R right) {
      return left.doubleValue() * ((Number) right).doubleValue();
    }

    @Override
    public <R> Number div(Number left, R right) {
      return left.doubleValue() / ((Number) right).doubleValue();
    }

    @Override
    public <R> Number mod(Number left, R right) {
      throw new RuntimeException("mod on double precision is not allowed, please use int");
    }

    @Override
    public <R> Number power(Number left, R right) {
      return Math.pow(left.doubleValue(), ((Number) right).doubleValue());
    }
  }

  public static class BigDecimalArithmetic extends Arithmetic<BigDecimal> {
    @Override
    public <R> BigDecimal add(BigDecimal left, R right) {
      return left.add((BigDecimal) right);
    }

    @Override
    public <R> BigDecimal sub(BigDecimal left, R right) {
      return left.subtract((BigDecimal) right);
    }

    @Override
    public <R> BigDecimal mul(BigDecimal left, R right) {
      return left.multiply((BigDecimal) right);
    }

    @Override
    public <R> BigDecimal div(BigDecimal left, R right) {
      return left.divide((BigDecimal) right, RoundingMode.HALF_DOWN);
    }

    @Override
    public <R> BigDecimal power(BigDecimal left, R right) {
      return left.pow(((Long) right).intValue());
    }
  }

  public static class StringArithmetic extends Arithmetic<String> {

    @Override
    public <R> String concat(String left, R right) {
      return left + right;
    }
  }
}
