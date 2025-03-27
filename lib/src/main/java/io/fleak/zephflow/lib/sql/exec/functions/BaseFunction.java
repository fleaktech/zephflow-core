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
import java.util.function.Function;
import lombok.Data;

@Data
public abstract class BaseFunction implements Function<List<Object>, Object> {

  private final String name;
  protected TypeSystem typeSystem;

  public BaseFunction(TypeSystem typeSystem, String name) {
    this.typeSystem = typeSystem;
    this.name = name;
  }

  public abstract Object apply(List<Object> args);

  @SuppressWarnings("all")
  public void assertArgs(List<Object> args, int number, String error) {
    if (args.size() != number) {
      throw new RuntimeException("function " + getName() + " argument mismatch; " + error);
    }
  }
}
