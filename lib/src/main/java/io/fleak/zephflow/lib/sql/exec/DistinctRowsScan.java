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

import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.IterableUtils;

/**
 * Takes a source and ensures only unique rows are returned. We can implement distinct iterators
 * that push down to the table level, but for in memory data it doesn't make much sense.
 */
public class DistinctRowsScan extends Table<Row> {

  private final Table<Row> source;

  public DistinctRowsScan(Table<Row> source) {
    this.source = source;
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return source.getHeader();
  }

  @Override
  public Iterator<Row> iterator() {
    return IterableUtils.uniqueIterable(source).iterator();
  }
}
