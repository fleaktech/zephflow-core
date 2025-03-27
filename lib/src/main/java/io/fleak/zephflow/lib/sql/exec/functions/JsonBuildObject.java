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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by bolei on 5/17/24 */
public class JsonBuildObject extends BaseFunction {
  public static final String NAME = "json_build_object";

  public JsonBuildObject(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    Preconditions.checkArgument(
        args.size() % 2 == 0,
        "function %s arg count should be even number but found %d",
        NAME,
        args.size());
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < args.size(); i += 2) {
      String key = typeSystem.lookupTypeCast(String.class).cast(args.get(i));
      Object value = args.get(i + 1);
      result.put(key, value);
    }
    return result;
  }
}
