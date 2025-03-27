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
