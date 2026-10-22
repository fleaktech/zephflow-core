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
package io.fleak.zephflow.lib.commands.stdin;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommandTest;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

class StdInSourceDecodingTest {

  @Test
  void decodingAloneDecodesEachLine() {
    StdInSourceCommand command = command();
    command.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(
            StdInSourceDto.Config.builder()
                .decoding(
                    new DecodingConfig(
                        List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT)))
                .build(),
            new TypeReference<>() {}));
    KafkaSourceCommandTest.TestSourceEventAcceptor acceptor =
        new KafkaSourceCommandTest.TestSourceEventAcceptor();

    InputStream stdin = System.in;
    try {
      System.setIn(
          new ByteArrayInputStream("{\"a\":1}\n{\"a\":2}\n\n".getBytes(StandardCharsets.UTF_8)));
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      command.execute("user", acceptor);
    } finally {
      System.setIn(stdin);
    }

    assertEquals(List.of(record(1), record(2)), acceptor.getReceivedEvents());
  }

  // stdin re-encodes each line as text, so the bytes go to the converter the command builds.
  @Test
  @SuppressWarnings("unchecked")
  void encodingTypeDecodesGzipBytesAsTheyAre() throws IOException {
    SourceExecutionContext<SerializedEvent> context =
        (SourceExecutionContext<SerializedEvent>)
            command()
                .createExecutionContext(
                    new MetricClientProvider.NoopMetricClientProvider(),
                    TestUtils.JOB_CONTEXT,
                    StdInSourceDto.Config.builder().encodingType(EncodingType.JSON_OBJECT).build(),
                    "node");
    SerializedEvent message = new SerializedEvent(null, gzip("{\"a\":1}"), null);

    assertEquals(
        "MALFORMED json message: JsonParseException",
        context.converter().convert(message, context).error().getMessage());
  }

  private static StdInSourceCommand command() {
    return new StdInSourceCommand(
        "node",
        TestUtils.JOB_CONTEXT,
        new JsonConfigParser<>(StdInSourceDto.Config.class),
        new StdInSourceConfigValidator());
  }

  private static RecordFleakData record(int a) {
    return (RecordFleakData) FleakData.wrap(Map.of("a", a));
  }

  private static byte[] gzip(String text) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
