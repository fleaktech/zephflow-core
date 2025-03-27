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
package io.fleak.zephflow.lib.sql.exec;

import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("unchecked")
public abstract class Catalog {

  public static Catalog fromMap(Map<String, Table<? extends Row>> tables) {
    return new InMemoryCatalog(tables);
  }

  public abstract <T extends Row> Table<T> resolve(String tableName);

  public static class InMemoryCatalog extends Catalog {

    final TreeMap<String, Table<? extends Row>> tables;

    public InMemoryCatalog(Map<String, Table<? extends Row>> tables) {
      this.tables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      this.tables.putAll(tables);
    }

    @Override
    public <T extends Row> Table<T> resolve(String tableName) {
      return (Table<T>) this.tables.get(tableName);
    }
  }
}
