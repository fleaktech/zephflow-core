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
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodeException;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Events through {@link IotHubMessageConverter} give the records written out here, merged metadata
 * fields and {@code _iothub} envelope included.
 */
class IotHubMessageConverterTest {

  private static final Map<String, String> METADATA =
      Map.of("deviceId", "sensor-01", "enqueuedTime", "2026-07-16T10:00:00Z", "prop.schema", "v2");
  private static final Map<String, Object> ENVELOPE =
      Map.of(
          "deviceId",
          "sensor-01",
          "enqueuedTime",
          "2026-07-16T10:00:00Z",
          "properties",
          Map.of("schema", "v2"));
  private static final Map<String, String> NO_PROPERTIES =
      Map.of("deviceId", "sensor-01", "enqueuedTime", "2026-07-16T10:00:00Z");

  static Stream<Arguments> cases() {
    byte[] key = "device-key".getBytes(StandardCharsets.UTF_8);
    String base64Key = Base64.getEncoder().encodeToString(key);
    return Stream.of(
        Arguments.of(
            "metadataAndProperties",
            EncodingType.JSON_OBJECT,
            event(null, "{\"temp\":72}", METADATA),
            List.of(
                record(
                    Map.of(
                        "temp", 72,
                        "deviceId", "sensor-01",
                        "enqueuedTime", "2026-07-16T10:00:00Z",
                        "prop.schema", "v2",
                        "_iothub", ENVELOPE)))),
        Arguments.of(
            "metadataWithoutProperties",
            EncodingType.JSON_OBJECT,
            event(null, "{\"temp\":72}", NO_PROPERTIES),
            List.of(
                record(
                    Map.of(
                        "temp",
                        72,
                        "deviceId",
                        "sensor-01",
                        "enqueuedTime",
                        "2026-07-16T10:00:00Z",
                        "_iothub",
                        Map.of(
                            "deviceId",
                            "sensor-01",
                            "enqueuedTime",
                            "2026-07-16T10:00:00Z",
                            "properties",
                            Map.of()))))),
        Arguments.of(
            "noMetadata",
            EncodingType.JSON_OBJECT,
            event(null, "{\"temp\":72}", null),
            List.of(record(Map.of("temp", 72)))),
        Arguments.of(
            "keyAndMetadata",
            EncodingType.JSON_OBJECT,
            event(key, "{\"temp\":72}", NO_PROPERTIES),
            List.of(
                record(
                    Map.of(
                        "temp",
                        72,
                        "deviceId",
                        "sensor-01",
                        "enqueuedTime",
                        "2026-07-16T10:00:00Z",
                        "__md_key",
                        base64Key,
                        "_iothub",
                        Map.of(
                            "deviceId",
                            "sensor-01",
                            "enqueuedTime",
                            "2026-07-16T10:00:00Z",
                            "properties",
                            Map.of()))))),
        Arguments.of(
            "keyWithoutMetadata",
            EncodingType.JSON_OBJECT,
            event(key, "{\"temp\":72}", null),
            List.of(record(Map.of("temp", 72)))),
        Arguments.of(
            "bodyFieldsUnderMetadataAndEnvelope",
            EncodingType.JSON_OBJECT,
            event(null, "{\"deviceId\":\"body\",\"_iothub\":\"body\",\"temp\":72}", METADATA),
            List.of(
                record(
                    Map.of(
                        "temp", 72,
                        "deviceId", "sensor-01",
                        "enqueuedTime", "2026-07-16T10:00:00Z",
                        "prop.schema", "v2",
                        "_iothub", ENVELOPE)))),
        Arguments.of(
            "severalJsonLines",
            EncodingType.JSON_OBJECT_LINE,
            event(null, "{\"temp\":1}\n{\"temp\":2}\n", METADATA),
            List.of(
                record(
                    Map.of(
                        "temp", 1,
                        "deviceId", "sensor-01",
                        "enqueuedTime", "2026-07-16T10:00:00Z",
                        "prop.schema", "v2",
                        "_iothub", ENVELOPE)),
                record(
                    Map.of(
                        "temp", 2,
                        "deviceId", "sensor-01",
                        "enqueuedTime", "2026-07-16T10:00:00Z",
                        "prop.schema", "v2",
                        "_iothub", ENVELOPE)))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void givesTheRecordsWithMetadataAndEnvelope(
      String name,
      EncodingType encodingType,
      SerializedEvent event,
      List<RecordFleakData> records) {
    assertEquals(records, decoded(encodingType, event).transformedData());
  }

  @Test
  void aMessageWithNoBodyGivesNoRecords() {
    SerializedEvent event = new SerializedEvent(null, null, new HashMap<>(METADATA));

    assertEquals(
        ConvertedResult.success(List.of(), event), decoded(EncodingType.JSON_OBJECT, event));
  }

  @Test
  void aBadMessageFails() {
    SerializedEvent event = event(null, "not json", METADATA);

    ConvertedResult<SerializedEvent> decoded = decoded(EncodingType.JSON_OBJECT, event);

    assertEquals(
        Arrays.asList(
            null,
            event,
            MessageDecodeException.class,
            "MALFORMED json message: JsonParseException"),
        Arrays.asList(
            decoded.transformedData(),
            decoded.sourceRecord(),
            decoded.error().getClass(),
            decoded.error().getMessage()));
  }

  private static ConvertedResult<SerializedEvent> decoded(
      EncodingType encodingType, SerializedEvent event) {
    return new IotHubMessageConverter(
            new MessageDecodingConverter(
                MessageDecodingConverter.decodingConfig(encodingType, null, false),
                MessageDecodingConverter.Unit.MESSAGE))
        .convert(event, context());
  }

  private static SerializedEvent event(byte[] key, String body, Map<String, String> metadata) {
    return new SerializedEvent(
        key,
        body.getBytes(StandardCharsets.UTF_8),
        metadata == null ? null : new HashMap<>(metadata));
  }

  private static RecordFleakData record(Map<String, Object> fields) {
    return (RecordFleakData) FleakData.wrap(fields);
  }

  private static SourceExecutionContext<SerializedEvent> context() {
    return new SourceExecutionContext<>(
        null,
        null,
        null,
        mock(FleakCounter.class),
        mock(FleakCounter.class),
        mock(FleakCounter.class),
        null);
  }
}
