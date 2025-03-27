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
import java.util.*;

public class RegexpSplitToTable extends BaseFunction {

  public static final String NAME = "regexp_split_to_table";

  public RegexpSplitToTable(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    assertArgs(args, 2, "(input, regex)");

    var input = args.get(0);
    if (input == null) return List.of();

    var regex = args.get(1);
    if (regex == null) return List.of(input);

    var inputStr = input.toString();
    if (inputStr.isBlank()) return List.of();

    var arr = inputStr.split(regex.toString());
    var out = new ArrayList<Map<String, String>>();
    for (var i = 0; i < arr.length; i++) {
      out.add(Map.of("row", arr[i]));
    }

    return out;
  }
}
