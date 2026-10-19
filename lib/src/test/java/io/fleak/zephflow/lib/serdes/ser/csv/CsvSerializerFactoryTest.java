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
package io.fleak.zephflow.lib.serdes.ser.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CsvSerializerFactoryTest {
  @Test
  void createSerializerFactory_passesCsvOptionsToSerializer() throws Exception {
    RecordFleakData event =
        (RecordFleakData) FleakData.wrap(Map.of("b", "2", "a", "1", "extra", "x"));

    byte[] bytes =
        SerializerFactory.createSerializerFactory(
                EncodingType.CSV, new CsvSerializerOptions(List.of("a", "b"), false))
            .createSerializer()
            .serialize(List.of(event))
            .value();

    assertEquals("1,2\r\n", new String(bytes, StandardCharsets.UTF_8));
  }

  @Test
  void createSerializerFactory_nullCsvOptionsFallBackToDefault() throws Exception {
    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("a", "1"));

    byte[] bytes =
        SerializerFactory.createSerializerFactory(EncodingType.CSV, null)
            .createSerializer()
            .serialize(List.of(event))
            .value();

    assertEquals("a\r\n1\r\n", new String(bytes, StandardCharsets.UTF_8));
  }
}
