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
package io.fleak.zephflow.lib.pathselect;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/19/24 */
class ValueExtractorTest {
  @Test
  public void regressionTestRecordPayloadExtractorImmutableMap() {
    ValueExtractor.RecordPayloadExtractor recordPayloadExtractor =
        new ValueExtractor.RecordPayloadExtractor(null, RuntimeException::new);
    Map<String, FleakData> payload =
        recordPayloadExtractor.doExtraction(new RecordFleakData(Map.of()));
    payload.put("k", new StringPrimitiveFleakData("v")); // should not crash
  }
}
