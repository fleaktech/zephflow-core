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

import io.fleak.zephflow.lib.sql.exec.types.BooleanLogics;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;

public abstract class Row implements Lookup {

  protected final Set<String> relationNames;

  /** A row can contain multiple relations. */
  protected Row(Set<String> relationNames) {

    this.relationNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    this.relationNames.addAll(relationNames);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Row row) {
      return Arrays.equals(getEntries(), row.getEntries());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getEntries());
  }

  public static Row empty(TypeSystem typeSystem) {
    return Row.wrapMap(typeSystem, Set.of("default"), new LinkedHashMap<>());
  }

  public Set<String> getRelationNames() {
    return relationNames;
  }

  public static Entry findEntry(Key key, Entry[] entries) {
    for (Entry entry : entries) {
      if (entry.key.equals(key)) return entry;
    }

    return null;
  }

  public static Row wrap(
      TypeSystem typeSystem, Set<String> relationNames, List<Row.Entry> entries) {
    var m = new LinkedHashMap<Key, Value>();
    entries.forEach(e -> m.put(e.key, e.value));
    return wrapMultiRelMap(typeSystem, relationNames, m);
  }

  public static Row wrapMultiRelMap(
      TypeSystem typeSystem, Set<String> relNames, LinkedHashMap<Key, Value> data) {
    if (relNames.size() == 1) {
      return wrapMap(typeSystem, relNames, data);
    }

    // else use combined map
    var tableRowKVMap = new HashMap<String, LinkedHashMap<Key, Value>>();
    // group rows by table name
    for (var entry : data.entrySet()) {
      var key = entry.getKey();
      var rel = key.rel();
      var rowMap = tableRowKVMap.computeIfAbsent(rel, k -> new LinkedHashMap<>());
      rowMap.put(key, entry.getValue());
    }

    // create a final Row map
    var tableRowMap = new HashMap<String, Row>();
    for (var entry : tableRowKVMap.entrySet()) {
      tableRowMap.put(
          entry.getKey(), Row.wrapMap(typeSystem, Set.of(entry.getKey()), entry.getValue()));
    }

    return new CombinedTableRow(tableRowMap.keySet(), tableRowMap);
  }

  public abstract Map<String, Row> rowMap();

  /**
   * Convert the row's entries to a map. When two columns have the same name, the first one is
   * written without the table prefix and the second one is written using the table prefix.
   */
  public Map<String, Object> asMap() {
    var m = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (var entry : getEntries()) {
      var k = entry.key();
      var prevVal = m.get(k.name());
      if (prevVal != null) {
        m.put(k.getQualifiedName(), entry.value().asObject());
      } else {
        m.put(k.name(), entry.value().asObject());
      }
    }

    return m;
  }

  public static Entry asEntry(Key key, Value value) {
    return new Entry(key, value);
  }

  public static Key asKey(String rel, String name, int pos) {
    return new Key(rel, name, pos);
  }

  public static Value asValue(Object v) {
    return new JavaValue(v);
  }

  public static Row wrapMap(
      TypeSystem typeSystem, Set<String> relNames, LinkedHashMap<Key, Value> data) {
    return new MapRow(typeSystem, relNames, data);
  }

  /**
   * The relation names cannot be derived from the rows. Some operations like lateral joins could
   * cause null or empty rows in which case the names will be present, but no row data.
   */
  public static Row combineRows(Set<String> relationNames, List<Row> rows) {
    Map<String, Row> rowMap = new HashMap<>();
    for (Row r : rows) {
      if (r != null) rowMap.putAll(r.rowMap());
    }

    return new CombinedTableRow(relationNames, rowMap);
  }

  public static Row combineRows(Set<String> relationNames, Row r1, Row r2) {

    var rowMap = new HashMap<>(r1.rowMap());

    for (var entry : r2.rowMap().entrySet()) {
      var v = rowMap.get(entry.getKey());
      if (v != null) {
        rowMap.put(entry.getKey(), v.assocEntries(entry.getValue().getEntries()));
      } else {
        rowMap.put(entry.getKey(), entry.getValue());
      }
    }

    return new CombinedTableRow(relationNames, rowMap);
  }

  public abstract Row assocEntries(Entry[] entries);

  /**
   * Creates a new instance of the Row with the column added. This operation should not mutate the
   * same row instance.
   */
  public abstract Row assocColumn(Row.Key key, Row.Value value);

  public abstract Row.Entry[] getEntries();

  public abstract Row renameColumns(List<Algebra.RenameField> renames);

  /**
   * @param relativePosition used for ordering only
   */
  public record Key(String rel, String name, int relativePosition) {

    /**
     * @return relation + "." + name
     */
    public String getQualifiedName() {
      return rel + "." + name;
    }

    public static boolean equalsIgnoreCase(String a, String b) {
      return (a == b) || (a != null && a.equalsIgnoreCase(b));
    }

    public boolean equals(String rel, String name) {
      return equalsIgnoreCase(this.rel, rel) && equalsIgnoreCase(this.name, name);
    }

    @Override
    public String toString() {
      return "Key{"
          + "rel='"
          + rel
          + '\''
          + ", name='"
          + name
          + '\''
          + ", relativePosition="
          + relativePosition
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Key key = (Key) o;
      return equals(key.rel, key.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          rel == null ? null : rel.toLowerCase(), name == null ? null : name.toLowerCase());
    }
  }

  public abstract static class Value {
    public abstract Object asObject();
  }

  public static class JavaValue extends Value {
    final Object v;

    public JavaValue(Object v) {
      this.v = v;
    }

    @Override
    public Object asObject() {
      return v;
    }

    @Override
    public String toString() {
      return "JavaValue{" + "v=" + v + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JavaValue javaValue = (JavaValue) o;
      if (v instanceof Double d && javaValue.v instanceof Number n) {
        return BooleanLogics.doubleValue().equal(d, n);
      } else if (v instanceof Float f && javaValue.v instanceof Number n) {
        return BooleanLogics.doubleValue().equal(f.doubleValue(), n);
      } else if (v instanceof Number n1 && javaValue.v instanceof Number n2) {
        return BooleanLogics.longValue().equal(n1, n2);
      }

      return Objects.equals(v, javaValue.v);
    }

    @Override
    public int hashCode() {
      return Objects.hash(v);
    }
  }

  public record Entry(Key key, Value value) {
    @Override
    public String toString() {
      return "Entry{" + "key=" + key + ", value=" + value + '}';
    }
  }
}
