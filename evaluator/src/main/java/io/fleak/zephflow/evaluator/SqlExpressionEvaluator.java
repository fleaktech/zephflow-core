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
package io.fleak.zephflow.evaluator;

import static io.fleak.zephflow.lib.commands.sql.SQLEvalCommand.EVENT_TABLE_NAME;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.exec.Catalog;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import java.util.List;
import java.util.Map;

/** Created by bolei on 1/10/25 */
public class SqlExpressionEvaluator extends ExpressionEvaluator {
  @Override
  protected FleakData doEvaluation(String expression, RecordFleakData event) {
    SQLInterpreter sqlInterpreter = SQLInterpreter.defaultInterpreter();
    SQLInterpreter.CompiledQuery query = sqlInterpreter.compileQuery(expression);
    var typeSystem = sqlInterpreter.getTypeSystem();
    List<FleakData> output =
        sqlInterpreter
            .eval(
                Catalog.fromMap(
                    Map.of(
                        EVENT_TABLE_NAME,
                        Table.ofListOfMaps(typeSystem, EVENT_TABLE_NAME, List.of(event.unwrap())))),
                query)
            .map(Row::asMap)
            .map(FleakData::wrap)
            .toList();
    return new ArrayFleakData(output);
  }
}
