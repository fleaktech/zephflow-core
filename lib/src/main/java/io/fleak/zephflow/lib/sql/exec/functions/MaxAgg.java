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
public class MaxAgg extends BaseFunction implements AggregateFunction<Object, Object> {

  public static final String NAME = "max";

  private final SQLAggCompare comparator;

  public MaxAgg(TypeSystem typeSystem) {
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
  public Object update(Object currentMax, List<Object> arguments) {
    var m2 = arguments.stream().filter(Objects::nonNull).max(comparator);
    if (m2.isPresent()) {
      var v2 = m2.get();
      if (currentMax == null || comparator.compare(v2, currentMax) > 0) return v2;
    }

    return currentMax;
  }

  @Override
  public Object combine(Object max1, Object max2) {
    return comparator.compare(max1, max2) > 0 ? max1 : max2;
  }

  @Override
  public Object getResult(Object state) {
    return state;
  }
}
