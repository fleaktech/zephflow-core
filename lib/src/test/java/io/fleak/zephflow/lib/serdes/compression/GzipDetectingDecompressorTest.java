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
package io.fleak.zephflow.lib.serdes.compression;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

class GzipDetectingDecompressorTest {

  private final GzipDetectingDecompressor decompressor = new GzipDetectingDecompressor();

  @Test
  void gunzipsAGzipPayloadKeepingKeyAndMetadata() throws IOException {
    byte[] key = "k".getBytes(StandardCharsets.UTF_8);
    Map<String, String> metadata = Map.of("topic", "otlp");

    SerializedEvent event =
        decompressor.decompress(new SerializedEvent(key, gzip("{\"a\":1}"), metadata));

    assertEquals("{\"a\":1}", new String(event.value(), StandardCharsets.UTF_8));
    assertArrayEquals(key, event.key());
    assertEquals(metadata, event.metadata());
  }

  @Test
  void passesAnUncompressedPayloadThroughUnchanged() {
    SerializedEvent original =
        new SerializedEvent(null, "{\"a\":1}".getBytes(StandardCharsets.UTF_8), Map.of());

    assertSame(original, decompressor.decompress(original));
  }

  @Test
  void passesNullShortAndEmptyPayloadsThrough() {
    for (byte[] value : new byte[][] {null, new byte[0], new byte[] {0x1f}}) {
      SerializedEvent original = new SerializedEvent(null, value, Map.of());
      assertSame(original, decompressor.decompress(original));
    }
  }

  @Test
  void rejectsATruncatedGzipPayload() throws IOException {
    byte[] compressed = gzip("{\"a\":1}");
    byte[] truncated = java.util.Arrays.copyOf(compressed, compressed.length / 2);

    assertThrows(
        Exception.class,
        () -> decompressor.decompress(new SerializedEvent(null, truncated, Map.of())));
  }

  private static byte[] gzip(String text) throws IOException {
    var bos = new ByteArrayOutputStream();
    try (var gzip = new GZIPOutputStream(bos)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return bos.toByteArray();
  }
}
