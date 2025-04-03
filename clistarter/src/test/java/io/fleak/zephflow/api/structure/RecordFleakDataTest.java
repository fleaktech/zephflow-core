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
package io.fleak.zephflow.api.structure;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/2/25 */
class RecordFleakDataTest {

  @Test
  void unwrap() {
    RecordFleakData recordFleakData = new RecordFleakData(new HashMap<>());
    recordFleakData.payload.put("k1", null);
    recordFleakData.payload.put(null, null);
    recordFleakData.payload.put("k3", new StringPrimitiveFleakData("v3"));

    var actual = recordFleakData.unwrap();
    var expected = new HashMap<String, Object>();
    expected.put("k1", null);
    expected.put(null, null);
    expected.put("k3", "v3");
    assertEquals(expected, actual);
  }
}
