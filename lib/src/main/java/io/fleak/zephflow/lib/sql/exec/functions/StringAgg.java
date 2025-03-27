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

import io.fleak.zephflow.lib.sql.exec.types.TypeCast;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.List;

/** Arguments: expression, separator */
public class StringAgg extends BaseFunction implements AggregateFunction<StringBuilder, String> {

  public static final String NAME = "string_agg";
  private TypeCast<String> toStringCast;

  public StringAgg(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public StringBuilder initState() {
    return new StringBuilder();
  }

  @Override
  public String getResult(StringBuilder state) {
    return state.toString();
  }

  @Override
  public StringBuilder combine(StringBuilder state1, StringBuilder state2) {
    return state1.append(state2);
  }

  @Override
  public StringBuilder update(StringBuilder state, List<Object> arguments) {
    // do not initialise in the constructor, due to how the type system is statically initialised.
    if (toStringCast == null) toStringCast = typeSystem.lookupTypeCast(String.class);
    if (arguments.size() != 2) {
      throw new RuntimeException("string_agg requires 2 arguments; expression and a separator.");
    }

    var s = toStringCast.cast(arguments.get(0));
    var sep = toStringCast.cast(arguments.get(1));

    if (state.isEmpty()) return state.append(s);

    return state.append(sep).append(s);
  }

  @Override
  public Object apply(List<Object> args) {
    throw new UnsupportedOperationException(
        "aggregation functions can only be used via the aggregation interface");
  }
}
