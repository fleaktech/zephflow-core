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
package io.fleak.zephflow.lib.commands.activemqsource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import jakarta.jms.ConnectionFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

/**
 * The converter this source's {@code createExecutionContext} builds: {@code decoding} alone decodes
 * messages, and {@code encodingType} decodes a value as it is, even one that starts with the gzip
 * magic bytes.
 */
class ActiveMqSourceDecodingTest {

  @Test
  void decodingAloneDecodesMessages() throws Exception {
    SourceExecutionContext<SerializedEvent> context =
        context(
            ActiveMqSourceDto.Config.builder()
                .brokerUrl("tcp://localhost:61616")
                .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
                .destination("q")
                .decoding(
                    new DecodingConfig(
                        List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
                .build());
    SerializedEvent message =
        new SerializedEvent(null, "{\"a\":1}\n{\"a\":2}".getBytes(StandardCharsets.UTF_8), null);

    assertEquals(
        ConvertedResult.success(List.of(record(1), record(2)), message),
        context.converter().convert(message, context));
  }

  @Test
  void encodingTypeDecodesGzipBytesAsTheyAre() throws Exception {
    SourceExecutionContext<SerializedEvent> context =
        context(
            ActiveMqSourceDto.Config.builder()
                .brokerUrl("tcp://localhost:61616")
                .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
                .destination("q")
                .encodingType(EncodingType.JSON_OBJECT)
                .build());
    SerializedEvent message = new SerializedEvent(null, gzip("{\"a\":1}"), null);

    assertEquals(
        "MALFORMED json message: JsonParseException",
        context.converter().convert(message, context).error().getMessage());
  }

  @SuppressWarnings("unchecked")
  private static SourceExecutionContext<SerializedEvent> context(ActiveMqSourceDto.Config config)
      throws Exception {
    JmsConnectionFactoryProvider provider = mock(JmsConnectionFactoryProvider.class);
    when(provider.createConnectionFactory(any()))
        .thenReturn(mock(ConnectionFactory.class, RETURNS_DEEP_STUBS));
    ActiveMqSourceCommand command =
        new ActiveMqSourceCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(ActiveMqSourceDto.Config.class),
            new ActiveMqSourceConfigValidator(),
            provider);
    return (SourceExecutionContext<SerializedEvent>)
        command.createExecutionContext(
            new MetricClientProvider.NoopMetricClientProvider(),
            TestUtils.JOB_CONTEXT,
            config,
            "node");
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
