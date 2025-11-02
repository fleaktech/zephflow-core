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
package io.fleak.zephflow.lib.commands.sql;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.sql.exec.Catalog;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import java.util.List;
import java.util.Map;

public class SQLEvalCommand extends ScalarCommand {

  public static final String EVENT_TABLE_NAME = "events";

  protected SQLEvalCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      SqlCommandInitializerFactory sqlCommandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, sqlCommandInitializerFactory);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SQL_EVAL;
  }

  @Override
  public ScalarCommand.ProcessResult process(
      List<RecordFleakData> events, String callingUser, ExecutionContext context) {
    Map<String, String> callingUserTagAndEventTags =
        getCallingUserTagAndEventTags(callingUser, events.isEmpty() ? null : events.get(0));
    SqlExecutionContext sqlContext = (SqlExecutionContext) context;
    sqlContext.getInputMessageCounter().increase(events.size(), callingUserTagAndEventTags);

    var typeSystem = sqlContext.getSqlInterpreter().getTypeSystem();

    try {
      List<RecordFleakData> output =
          sqlContext
              .getSqlInterpreter()
              .eval(
                  Catalog.fromMap(
                      Map.of(
                          EVENT_TABLE_NAME,
                          Table.ofListOfMaps(
                              typeSystem,
                              EVENT_TABLE_NAME,
                              events.stream().map(RecordFleakData::unwrap).toList()))),
                  sqlContext.getQuery())
              .map(Row::asMap)
              .map(m -> (RecordFleakData) FleakData.wrap(m))
              .toList();
      sqlContext.getOutputMessageCounter().increase(output.size(), callingUserTagAndEventTags);
      return new ProcessResult(output, List.of());
    } catch (Exception e) {
      sqlContext.getErrorCounter().increase(events.size(), callingUserTagAndEventTags);
      List<ErrorOutput> errorOutputs =
          events.stream().map(event -> new ErrorOutput(event, e.getMessage())).toList();
      return new ProcessResult(List.of(), errorOutputs);
    }
  }

  @Override
  public List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) throws Exception {
    throw new IllegalAccessException("this method shouldn't be accessed");
  }
}
