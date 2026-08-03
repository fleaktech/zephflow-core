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
package io.fleak.zephflow.lib.pathselect;

import io.fleak.zephflow.api.structure.*;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Created by bolei on 3/6/24 Given a FleakData object, extract the Java Object value inside */
public abstract class ValueExtractor<T> {
  protected final T defaultValue;
  protected final Supplier<RuntimeException> exceptionSupplier;

  public ValueExtractor(T defaultValue, Supplier<RuntimeException> exceptionSupplier) {
    this.defaultValue = defaultValue;
    this.exceptionSupplier = exceptionSupplier;
  }

  public T extractValue(FleakData fleakData) {
    if (!typeMatches(fleakData)) {
      return handleError();
    }

    return doExtraction(fleakData);
  }

  protected abstract T doExtraction(FleakData fleakData);

  protected abstract boolean typeMatches(FleakData fleakData);

  public T handleError() {
    if (exceptionSupplier == null) {
      return defaultValue;
    }
    throw exceptionSupplier.get();
  }

  public static class StringValueExtractor extends ValueExtractor<String> {

    public StringValueExtractor(String defaultValue, Supplier<RuntimeException> exceptionSupplier) {
      super(defaultValue, exceptionSupplier);
    }

    @Override
    protected String doExtraction(FleakData fleakData) {
      return fleakData.getStringValue();
    }

    @Override
    protected boolean typeMatches(FleakData fleakData) {
      return fleakData instanceof StringPrimitiveFleakData;
    }
  }

  /**
   * Extracts any scalar (string, number, boolean) as its string form. Records and arrays are not
   * scalars and fall through to {@link #handleError()}, as does a primitive holding a null value.
   *
   * <p>Numbers are rendered in plain notation with trailing zeros stripped, so equal numbers always
   * produce equal strings regardless of how they were typed: {@code 4}, {@code 4.0} and a {@code
   * LONG}-typed 4 all yield {@code "4"}, and {@code 1e20} yields {@code "100000000000000000000"}
   * rather than {@code "1.0E20"}. That matters because these strings are used as routing keys — two
   * spellings of one id would otherwise split across partitions.
   *
   * <p>Note that {@link NumberPrimitiveFleakData} is double-backed, so integers beyond 2^53 are
   * already rounded before they reach here; two distinct ids that large can collapse onto one key.
   * The serialized record body has the same limitation, so keys stay consistent with payloads.
   */
  public static class ScalarStringValueExtractor extends ValueExtractor<String> {

    public ScalarStringValueExtractor(
        String defaultValue, Supplier<RuntimeException> exceptionSupplier) {
      super(defaultValue, exceptionSupplier);
    }

    @Override
    protected String doExtraction(FleakData fleakData) {
      Object raw = fleakData.unwrap();
      if (raw == null) {
        return handleError();
      }
      if (raw instanceof Number) {
        return new BigDecimal(raw.toString()).stripTrailingZeros().toPlainString();
      }
      return String.valueOf(raw);
    }

    @Override
    protected boolean typeMatches(FleakData fleakData) {
      return fleakData instanceof PrimitiveFleakData;
    }
  }

  public static class FloatValueExtractor extends ValueExtractor<Float> {

    public FloatValueExtractor(Float defaultValue, Supplier<RuntimeException> exceptionSupplier) {
      super(defaultValue, exceptionSupplier);
    }

    @Override
    protected Float doExtraction(FleakData fleakData) {
      return (float) fleakData.getNumberValue();
    }

    @Override
    protected boolean typeMatches(FleakData fleakData) {
      return fleakData instanceof NumberPrimitiveFleakData;
    }
  }

  public static class RecordPayloadExtractor extends ValueExtractor<Map<String, FleakData>> {

    public RecordPayloadExtractor(
        Map<String, FleakData> defaultValue, Supplier<RuntimeException> exceptionSupplier) {
      super(defaultValue, exceptionSupplier);
    }

    @Override
    protected Map<String, FleakData> doExtraction(FleakData fleakData) {
      return new HashMap<>(fleakData.getPayload());
    }

    @Override
    protected boolean typeMatches(FleakData fleakData) {
      return fleakData instanceof RecordFleakData;
    }
  }

  public static class ArrayPayloadExtractor extends ValueExtractor<List<FleakData>> {
    public ArrayPayloadExtractor(
        List<FleakData> defaultValue, Supplier<RuntimeException> exceptionSupplier) {
      super(defaultValue, exceptionSupplier);
    }

    @Override
    protected List<FleakData> doExtraction(FleakData fleakData) {
      return fleakData.getArrayPayload();
    }

    @Override
    protected boolean typeMatches(FleakData fleakData) {
      return fleakData instanceof ArrayFleakData;
    }
  }

  public static class ArrayValueExtractor<T> extends ValueExtractor<List<T>> {
    private final ValueExtractor<T> innerValueExtractor;

    public ArrayValueExtractor(
        List<T> defaultValue,
        Supplier<RuntimeException> exceptionSupplier,
        ValueExtractor<T> innerValueExtractor) {
      super(defaultValue, exceptionSupplier);
      this.innerValueExtractor = innerValueExtractor;
    }

    @Override
    protected List<T> doExtraction(FleakData fleakData) {
      List<FleakData> arrayValue = fleakData.getArrayPayload();
      return arrayValue.stream().map(innerValueExtractor::extractValue).toList();
    }

    @Override
    protected boolean typeMatches(FleakData fleakData) {
      return fleakData instanceof ArrayFleakData;
    }
  }
}
