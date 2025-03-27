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
import java.util.Map;

public class JsonGetToJson extends BaseFunction {

  public static final String NAME = "jsonGetToJson";

  public JsonGetToJson(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    assertArgs(args, 2, "field-name, datum");
    var key = args.get(0);
    var datum = args.get(1);
    if (key == null) return null;
    if (datum == null) return null;

    if (key instanceof String) {
      try {
        var datumMap = typeSystem.lookupTypeCast(Map.class).cast(datum);
        return datumMap.get(key);
      } catch (Throwable t) {
        return null;
      }
    } else if (key instanceof Number k) {
      try {
        var datumMap = typeSystem.lookupTypeCast(List.class).cast(datum);
        return datumMap.get((k.intValue()));
      } catch (Throwable t) {
        return null;
      }
    }
    return null;
  }
}
