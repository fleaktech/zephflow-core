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
package io.fleak.zephflow.lib.commands.sink;

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.format;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig.Mode;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SinkEncodingTest {

  private static final WriterCapability CAPABILITY =
      new WriterCapability(
          "testsink", Unit.ONE_RECORD, Set.of("json"), Set.of(Mode.OBJECT), false, Set.of());

  @Test
  void encodingIsValid() {
    assertDoesNotThrow(
        () -> SinkEncoding.validate(format("{\"type\":\"json\",\"mode\":\"object\"}"), CAPABILITY));
  }

  @Test
  void missingEncodingIsRefused() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SinkEncoding.validate(null, CAPABILITY));
    assertEquals("testsink: encoding is required", e.getMessage());
  }

  @Test
  void encodingIsCheckedAgainstTheCapability() {
    EncodingConfig csv = format("{\"type\":\"csv\"}");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> SinkEncoding.validate(csv, CAPABILITY));
    assertEquals("testsink cannot write csv", e.getMessage());
  }

  @Test
  void jsonObjectWritesTheRecordAsOneJsonObject() throws Exception {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("id", 1);
    RecordFleakData record = (RecordFleakData) FleakData.wrap(payload);
    assertEquals(
        "{\"id\":1}", new String(SinkEncoding.JSON_OBJECT.encode(record), StandardCharsets.UTF_8));
  }
}
