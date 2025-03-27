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
package io.fleak.zephflow.lib.utils;

import io.fleak.zephflow.api.structure.FleakData;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class XmlUtilsTest {

  @Test
  public void testReadXml() {
    String doc =
        """
                <doc><person><name>ABC1</name></person><person><name>ABC2</name></person></doc>
                """;
    var record = XmlUtils.loadFleakDataFromXMLString(doc);
    System.out.println(record);

    Assertions.assertEquals(
        FleakData.wrap(Map.of("person", List.of(Map.of("name", "ABC1"), Map.of("name", "ABC2")))),
        record);
  }
}
