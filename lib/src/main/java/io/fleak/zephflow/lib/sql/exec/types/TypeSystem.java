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
package io.fleak.zephflow.lib.sql.exec.types;

import java.util.List;
import java.util.function.Function;

public abstract class TypeSystem {

  public abstract <T> Arithmetic<T> lookupTypeArithmetic(Class<?> cls);

  public abstract <T> BooleanLogic<T> lookupTypeBoolean(Class<?> cls);

  public abstract <T> TypeCast<T> lookupTypeCast(Class<T> clz);

  public abstract Function<List<Object>, Object> lookupFunction(String name);
}
