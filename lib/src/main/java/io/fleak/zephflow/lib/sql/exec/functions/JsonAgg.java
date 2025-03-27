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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <code>
 * SELECT
 *     category,
 *     json_agg(jsonb_build_object('key', key, 'value', value)) AS json_data
 * FROM
 *     my_table
 * GROUP BY
 *     category;
 * </code>
 */
public class JsonAgg extends BaseFunction
    implements AggregateFunction<ArrayList<Map<Object, Object>>, ArrayList<Map<Object, Object>>> {

  public static final String NAME = "json_agg";

  public JsonAgg(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public ArrayList<Map<Object, Object>> initState() {
    return new ArrayList<>();
  }

  @Override
  public ArrayList<Map<Object, Object>> update(
      ArrayList<Map<Object, Object>> state, List<Object> arguments) {

    if (arguments.isEmpty()) return state;

    if (arguments.size() > 1) {
      throw new RuntimeException(
          "json_agg only accepts one argument; " + arguments.size() + " arguments given");
    }
    var arg = arguments.get(0);
    if (arg instanceof Map m) {
      state.add(m);
    } else {
      throw new RuntimeException(
          "invalid argument " + arg + ", use json_build_object or other to create an object first");
    }

    return state;
  }

  @Override
  public ArrayList<Map<Object, Object>> combine(
      ArrayList<Map<Object, Object>> state1, ArrayList<Map<Object, Object>> state2) {
    var retList = new ArrayList<Map<Object, Object>>(state1);
    retList.addAll(state2);
    return retList;
  }

  @Override
  public ArrayList<Map<Object, Object>> getResult(ArrayList<Map<Object, Object>> state) {
    return state;
  }

  @Override
  public Object apply(List<Object> args) {
    throw new UnsupportedOperationException(
        "aggregation functions can only be used via the aggregation interface");
  }
}
