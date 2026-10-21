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
package io.fleak.zephflow.lib.commands.mqttsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.junit.jupiter.api.Test;

/**
 * The converter this source's {@code createExecutionContext} builds: {@code decoding} alone decodes
 * messages, and {@code encodingType} decodes a value as it is, even one that starts with the gzip
 * magic bytes.
 */
class MqttSourceDecodingTest {

  @Test
  void decodingAloneDecodesMessages() throws Exception {
    SourceExecutionContext<SerializedEvent> context =
        context(
            MqttSourceDto.Config.builder()
                .brokerUrl("tcp://localhost:1883")
                .topicFilter("t")
                .clientId("c")
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
            MqttSourceDto.Config.builder()
                .brokerUrl("tcp://localhost:1883")
                .topicFilter("t")
                .clientId("c")
                .encodingType(EncodingType.JSON_OBJECT)
                .build());
    SerializedEvent message = new SerializedEvent(null, gzip("{\"a\":1}"), null);

    ConvertedResult<SerializedEvent> result = context.converter().convert(message, context);

    assertEquals(
        Arrays.asList(null, message, "MALFORMED json message: JsonParseException"),
        Arrays.asList(
            result.transformedData(), result.sourceRecord(), result.error().getMessage()));
  }

  @SuppressWarnings("unchecked")
  private static SourceExecutionContext<SerializedEvent> context(MqttSourceDto.Config built)
      throws Exception {
    MqttSourceDto.Config config =
        new JsonConfigParser<>(MqttSourceDto.Config.class)
            .parseConfig(OBJECT_MAPPER.convertValue(built, new TypeReference<>() {}));
    new MqttSourceConfigValidator().validateConfig(config, "node", TestUtils.JOB_CONTEXT);
    MqttClientProvider provider = mock(MqttClientProvider.class);
    when(provider.createClient(any())).thenReturn(mock(MqttClient.class));
    when(provider.buildConnectionOptions(any(), any())).thenReturn(new MqttConnectionOptions());
    MqttSourceCommand command =
        new MqttSourceCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(MqttSourceDto.Config.class),
            new MqttSourceConfigValidator(),
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
