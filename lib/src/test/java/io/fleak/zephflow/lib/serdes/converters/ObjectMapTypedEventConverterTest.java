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
package io.fleak.zephflow.lib.serdes.converters;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/18/24 */
class ObjectMapTypedEventConverterTest {

  @Test
  void typedEventToFleakData() {
    Map<String, Object> payload = Map.of("k", "v");
    TypedEventContainer<Map<String, Object>> tec = new TypedEventContainer<>(payload, null);
    ObjectMapTypedEventConverter converter = new ObjectMapTypedEventConverter();
    RecordFleakData recordFleakData = converter.typedEventToFleakData(tec);
    assertEquals(Map.of("k", new StringPrimitiveFleakData("v")), recordFleakData.getPayload());
  }
}
