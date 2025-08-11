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
package io.fleak.zephflow.lib.parser.extractions;


import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.Map;

/** Created by bolei on 5/6/25 */
public record JsonExtractionRule(JsonExtractionConfig jsonExtractionConfig)
    implements ExtractionRule {

  @Override
  public RecordFleakData extract(String raw) throws Exception {
    FleakData fleakData = JsonUtils.loadFleakDataFromJsonString(raw);
    return new RecordFleakData(Map.of(jsonExtractionConfig.getOutputFieldName(), fleakData));
  }
}
