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

import static io.fleak.zephflow.lib.commands.sink.SinkEncoding.field;
import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig.Mode;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SinkEncodingTest {

  private static final WriterCapability CAPABILITY =
      new WriterCapability(
          "testsink", Unit.ONE_RECORD, Set.of("json"), Set.of(Mode.OBJECT), false, Set.of());

  private static final EncodingConfig JSON_OBJECT =
      format("{\"type\":\"json\",\"mode\":\"object\"}");

  @Test
  void encodingAloneIsValid() {
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                JSON_OBJECT,
                CAPABILITY,
                field("encodingType", null),
                field("kvPairDelimiter", ""),
                field("kvFields", List.of()),
                field("avroSchema", Map.of())));
  }

  @Test
  void encodingWithLegacyFieldsIsRefusedNamingThem() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SinkEncoding.validate(
                    JSON_OBJECT,
                    CAPABILITY,
                    field("encodingType", "JSON_OBJECT"),
                    field("kvFields", List.of("a")),
                    field("kvPairDelimiter", "\t"),
                    field("kvCleanFields", false)));
    assertEquals(
        "testsink: encoding cannot be combined with encodingType, kvFields, kvPairDelimiter,"
            + " kvCleanFields",
        e.getMessage());
  }

  @Test
  void encodingIsCheckedAgainstTheCapability() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SinkEncoding.validate(
                    format("{\"type\":\"csv\"}"), CAPABILITY, field("encodingType", null)));
    assertEquals("testsink cannot write csv", e.getMessage());
  }

  @Test
  void legacySerializerWritesTheSameBytesAsARecordEncoder() throws Exception {
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1));
    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT).createSerializer();

    byte[] expected = serializer.serialize(List.of(record)).value();
    assertArrayEquals(expected, SinkEncoding.fromSerializer(serializer).encode(record));
  }
}
