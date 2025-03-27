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

/** Arguments: expression, separator */
public class CountAgg extends BaseFunction
    implements AggregateFunction<CountAgg.LongCounter, Long> {

  public static final String NAME = "count";

  public CountAgg(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public LongCounter initState() {
    return new LongCounter(0L);
  }

  @Override
  public Long getResult(CountAgg.LongCounter state) {
    return state.v;
  }

  @Override
  public LongCounter combine(LongCounter state1, LongCounter state2) {
    return new LongCounter(state1.v + state2.v);
  }

  @Override
  public LongCounter update(LongCounter state, List<Object> arguments) {
    // we ignore the arguments in count
    state.v++;
    return state;
  }

  @Override
  public Object apply(List<Object> args) {
    throw new UnsupportedOperationException(
        "aggregation functions can only be used via the aggregation interface");
  }

  public static class LongCounter {
    long v;

    public LongCounter(long v) {
      this.v = v;
    }
  }
}
