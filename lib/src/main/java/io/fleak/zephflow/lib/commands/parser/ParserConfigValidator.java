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
package io.fleak.zephflow.lib.commands.parser;

import static io.fleak.zephflow.api.ConfigurationValidationException.at;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.parser.extractions.WindowsMultilineExtractionConfig;
import java.time.DateTimeException;
import java.time.ZoneId;

public class ParserConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    validate((ParserConfigs.ParserConfig) commandConfig, ConfigurationPath.root());
  }

  private void validate(ParserConfigs.ParserConfig parserConfig, ConfigurationPath path) {
    require(
        parserConfig.getTargetField() != null, path.field("targetField"), "targetField is missing");
    var targetPath = parserConfig.getTargetFieldPath();
    if (targetPath != null) {
      require(
          !targetPath.isEmpty(),
          path.field("targetFieldPath"),
          "targetFieldPath must not be empty");
      for (int index = 0; index < targetPath.size(); index++) {
        require(
            targetPath.get(index) != null,
            path.field("targetFieldPath").index(index),
            "targetFieldPath keys must not be null");
      }
    }
    require(
        parserConfig.getExtractionConfig() != null,
        path.field("extractionConfig"),
        "extractionConfig is missing");
    if (parserConfig.getExtractionConfig() instanceof WindowsMultilineExtractionConfig windows
        && windows.getTimezone() != null) {
      try {
        ZoneId.of(windows.getTimezone());
      } catch (DateTimeException failure) {
        throw at(
            path.field("extractionConfig").field("timezone"),
            failure,
            "timezone is not a recognised time zone name or offset");
      }
    }
    var dispatch = parserConfig.getDispatchConfig();
    if (dispatch != null) {
      var dispatchPath = path.field("dispatchConfig");
      if (dispatch.getDispatchMap() != null) {
        dispatch
            .getDispatchMap()
            .forEach(
                (key, rule) -> {
                  if (rule != null) validate(rule, dispatchPath.field("dispatchMap").field(key));
                });
      }
      if (dispatch.getDefaultConfig() != null) {
        validate(dispatch.getDefaultConfig(), dispatchPath.field("defaultConfig"));
      }
    }
  }
}
