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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

class MappedMessageConverterTest {

  record Message(String body, String id) {}

  @Test
  void successKeepsTheOriginalRecordAndNeverDescribesIt() {
    Message message = new Message("{\"a\":1}\n{\"a\":2}", "m-1");
    MappedMessageConverter<Message> converter =
        converter(
            m -> {
              throw new AssertionError("described " + m.id());
            });

    assertEquals(
        ConvertedResult.success(List.of(record(1), record(2)), message),
        converter.convert(message, context()));
  }

  @Test
  void failureKeepsTheExceptionAndTheOriginalRecordAndLogsTheDescription() {
    Message message = new Message("{\"a\":1}\nnot json", "m-2");

    ConvertedResult<Message> result;
    List<String> errors;
    try (BoundedLogCapture logs = new BoundedLogCapture(MessageDecodingConverter.class)) {
      result = converter(m -> "test messageId: " + m.id()).convert(message, context());
      errors =
          logs.events().stream()
              .filter(e -> e.getLevel() == Level.ERROR)
              .map(e -> e.getMessage().getFormattedMessage())
              .toList();
    }

    assertEquals(
        Arrays.asList(
            null,
            message,
            MessageDecodeException.class,
            "MALFORMED json message: JsonParseException",
            List.of(
                "failed to deserialize event: MALFORMED json message: JsonParseException\n"
                    + "test messageId: m-2")),
        Arrays.asList(
            result.transformedData(),
            result.sourceRecord(),
            result.error().getClass(),
            result.error().getMessage(),
            errors));
  }

  private static MappedMessageConverter<Message> converter(Function<Message, String> description) {
    return new MappedMessageConverter<>(
        new MessageDecodingConverter(
            new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)),
            MessageDecodingConverter.Unit.MESSAGE),
        m -> new SerializedEvent(null, m.body().getBytes(StandardCharsets.UTF_8), null),
        description);
  }

  private static SourceExecutionContext<Message> context() {
    return new SourceExecutionContext<>(
        null,
        null,
        null,
        mock(FleakCounter.class),
        mock(FleakCounter.class),
        mock(FleakCounter.class),
        null);
  }

  private static RecordFleakData record(int a) {
    return (RecordFleakData) FleakData.wrap(Map.of("a", a));
  }
}
