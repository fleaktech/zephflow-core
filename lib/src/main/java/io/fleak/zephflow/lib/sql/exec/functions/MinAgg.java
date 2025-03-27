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
package io.fleak.zephflow.lib.sql.exec.functions;

import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.List;
import java.util.Objects;

/** Arguments: expression, separator */
public class MinAgg extends BaseFunction implements AggregateFunction<Object, Object> {

  public static final String NAME = "min";

  private final SQLAggCompare comparator;

  public MinAgg(TypeSystem typeSystem) {
    super(typeSystem, NAME);
    this.comparator = new SQLAggCompare(typeSystem);
  }

  @Override
  public Object apply(List<Object> args) {
    throw new UnsupportedOperationException(
        "aggregation functions can only be used via the aggregation interface");
  }

  @Override
  public Object initState() {
    return null;
  }

  @Override
  public Object update(Object currentMin, List<Object> arguments) {
    var m2 = arguments.stream().filter(Objects::nonNull).min(comparator);
    if (m2.isPresent()) {
      var v2 = m2.get();
      if (currentMin == null || comparator.compare(v2, currentMin) < 0) return v2;
    }

    return currentMin;
  }

  @Override
  public Object combine(Object min1, Object min2) {
    return comparator.compare(min1, min2) < 0 ? min1 : min2;
  }

  @Override
  public Object getResult(Object state) {
    return state;
  }
}
