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
package io.fleak.zephflow.lib.commands.kinesissource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

/**
 * Kinesis has no client seam, so its {@code decoding} is exercised by the LocalStack test; this
 * pins that {@code encodingType} decodes a value as it is, even one that starts with the gzip magic
 * bytes.
 */
class KinesisSourceDecodingTest {

  @Test
  void encodingTypeDecodesGzipBytesAsTheyAre() throws IOException {
    MessageDecodingConverter converter =
        new MessageDecodingConverter(
            KinesisSourceCommand.decodingConfig(
                KinesisSourceDto.Config.builder()
                    .applicationName("app")
                    .regionStr("us-east-1")
                    .streamName("stream")
                    .encodingType(EncodingType.JSON_OBJECT)
                    .build()),
            MessageDecodingConverter.Unit.MESSAGE);
    SerializedEvent message = new SerializedEvent(null, gzip("{\"a\":1}"), null);

    assertEquals(
        "MALFORMED json message: JsonParseException",
        converter
            .convert(
                message,
                new SourceExecutionContext<>(
                    null,
                    null,
                    null,
                    mock(FleakCounter.class),
                    mock(FleakCounter.class),
                    mock(FleakCounter.class),
                    null))
            .error()
            .getMessage());
  }

  private static byte[] gzip(String text) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
