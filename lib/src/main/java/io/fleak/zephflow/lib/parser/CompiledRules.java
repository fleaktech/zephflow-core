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
package io.fleak.zephflow.lib.parser;

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_ERR;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.extractions.ExtractionRule;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.NonNull;

/**
 * Created by bolei on 10/14/24
 *
 * <p>Compiled fromParserConfigs
 */
public interface CompiledRules {

  record ParseRule(
      @NonNull String targetField,
      boolean removeTargetField,
      @NonNull ExtractionRule extractionRule,
      DispatchRule dispatchRule) {

    public RecordFleakData parse(RecordFleakData input) {
      FleakData targetValue = input.getPayload().get(targetField);
      if (targetValue == null) {
        return input;
      }
      String strVal = targetValue.getStringValue();
      RecordFleakData parsedData;
      try {
        parsedData = extractionRule.extract(strVal);
      } catch (Exception e) {
        return input.copyAndMerge(
            Map.of(FIELD_NAME_ERR, Objects.requireNonNull(FleakData.wrap(e.getMessage()))));
      }
      RecordFleakData mergedRecord = input.copyAndMerge(parsedData.getPayload());
      if (removeTargetField) {
        mergedRecord.getPayload().remove(targetField);
      }

      if (dispatchRule == null) {
        return mergedRecord;
      }

      FleakData dispatchValue = mergedRecord.getPayload().get(dispatchRule.dispatchField);

      ParseRule nextParseRule = null;
      if (dispatchValue != null) {
        String dispatchValueStr = dispatchValue.getStringValue();
        nextParseRule =
            Optional.ofNullable(dispatchRule.dispatchRulesMap.get(dispatchValueStr))
                .orElse(dispatchRule.defaultRule);
      }

      if (nextParseRule == null) {
        return mergedRecord;
      }

      return nextParseRule.parse(mergedRecord);
    }
  }

  record DispatchRule(
      @NonNull String dispatchField,
      @NonNull Map<String, ParseRule> dispatchRulesMap,
      ParseRule defaultRule) {}
}
