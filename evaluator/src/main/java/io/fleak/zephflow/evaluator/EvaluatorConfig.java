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

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.cli.*;

/** Created by bolei on 1/10/25 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EvaluatorConfig {
  private static final Options CLI_OPTIONS;
  private static final Option TYPE_OPT =
      Option.builder("t")
          .longOpt("type")
          .desc("expression type. available values: EVAL, EXPRESSION_SQL")
          .hasArg()
          .required(true)
          .build();

  private static final Option EXPR_OPT =
      Option.builder("el")
          .longOpt("expressionList")
          .desc("Base64 encoded expression string list")
          .hasArg()
          .required(true)
          .build();

  private static final Option INPUT_OPT =
      Option.builder("i")
          .longOpt("input")
          .desc("Base64 encoded json object as the input event")
          .hasArg()
          .required(true)
          .build();

  static {
    CLI_OPTIONS = new Options();
    CLI_OPTIONS.addOption(TYPE_OPT).addOption(EXPR_OPT).addOption(INPUT_OPT);
  }

  private AntlrUtils.GrammarType grammarType;
  private List<String> expressions;
  private RecordFleakData inputEvent;

  public static EvaluatorConfig parse(String[] args) throws ParseException {
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(CLI_OPTIONS, args);
    AntlrUtils.GrammarType type =
        getRequiredCommandArgValue(commandLine, "t", AntlrUtils.GrammarType::valueOf);
    List<String> expressionList =
        getRequiredCommandArgValue(
            commandLine,
            "el",
            e -> fromJsonString(new String(fromBase64String(e)), new TypeReference<>() {}));
    RecordFleakData input =
        getRequiredCommandArgValue(
            commandLine,
            "i",
            i -> {
              byte[] data = fromBase64String(i);
              var map = JsonUtils.fromJsonBytes(data, new TypeReference<Map<String, Object>>() {});
              return ((RecordFleakData) FleakData.wrap(map));
            });
    return EvaluatorConfig.builder()
        .grammarType(type)
        .expressions(expressionList)
        .inputEvent(input)
        .build();
  }
}
