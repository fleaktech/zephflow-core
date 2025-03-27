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
package io.fleak.zephflow.lib.sql.exec.functions.strings;

import io.fleak.zephflow.lib.sql.exec.functions.BaseFunction;
import io.fleak.zephflow.lib.sql.exec.types.TypeCast;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.List;

public class Substring extends BaseFunction {

  public static final String NAME = "substring";
  private static TypeCast<Integer> integerTypeCast;

  public Substring(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    if (integerTypeCast == null) integerTypeCast = typeSystem.lookupTypeCast(Integer.class);

    assertArgs(args, 3, "(input, start, length)");

    var input = args.get(0);
    if (input == null) return null;

    var start = integerTypeCast.cast(args.get(1)) - 1;
    var length = integerTypeCast.cast(args.get(2));

    return input.toString().substring(start, start + length);
  }
}
