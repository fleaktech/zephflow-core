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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

public class CompositeDecompressorTest {

  private byte[] gzip(byte[] input) throws IOException {
    var bos = new ByteArrayOutputStream();
    try (var gzipOut = new GZIPOutputStream(bos)) {
      gzipOut.write(input);
    }
    return bos.toByteArray();
  }

  @Test
  public void testNestedGzipDecompression() throws IOException {
    var originalText = "hello world";
    var onceCompressed = gzip(originalText.getBytes(StandardCharsets.UTF_8));
    var twiceCompressed = gzip(onceCompressed);

    var event = new SerializedEvent(null, twiceCompressed, Map.of("level", "2x"));

    var decompressor =
        new CompositeDecompressor(List.of(new GzipDecompressor(), new GzipDecompressor()));

    var result = decompressor.decompress(event);
    var decompressedText = new String(result.value(), StandardCharsets.UTF_8);

    assertEquals(originalText, decompressedText);
  }
}
