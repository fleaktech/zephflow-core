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
import java.util.Collection;
import java.util.List;

public class JsonArrayLength extends BaseFunction {
  public static final String NAME = "json_array_length";

  public JsonArrayLength(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    if (args.isEmpty()) return 0;

    var v1 = args.get(0);
    if (v1 instanceof Collection c) return c.size();
    if (v1.getClass().isArray()) {
      return java.lang.reflect.Array.getLength(v1);
    }

    return 0;
  }
}
