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

import io.fleak.zephflow.lib.sql.exec.types.Arithmetic;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.List;

/** Arguments: expression, separator */
public class SumAgg extends BaseFunction implements AggregateFunction<SumAgg.State, Object> {

  public static final String NAME = "sum";

  Arithmetic arithmetic;

  public SumAgg(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    throw new UnsupportedOperationException(
        "aggregation functions can only be used via the aggregation interface");
  }

  @Override
  public State initState() {
    return new State(0L);
  }

  @Override
  public State update(State state, List<Object> arguments) {
    if (arguments.isEmpty()) return state;

    for (Object argument : arguments) {
      if (argument == null) continue;
      if (arithmetic == null) {
        arithmetic = typeSystem.lookupTypeArithmetic(arguments.get(0).getClass());
      }

      state.v = arithmetic.add(argument, state.v);
      return state;
    }

    return state;
  }

  @Override
  public State combine(State state1, State state2) {
    if (arithmetic == null) {
      arithmetic = typeSystem.lookupTypeArithmetic(state1.v.getClass());
    }
    return new State(arithmetic.add(state1.v, state2.v));
  }

  @Override
  public Object getResult(State state) {
    return state.v;
  }

  public static final class State {
    Object v;

    public State(Object v) {
      this.v = v;
    }
  }
}
