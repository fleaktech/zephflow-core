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

public class GzipDecompressorTest {

  @Test
  public void testDecompress() throws IOException {
    var originalText = "Hello, this is a test string!";
    byte[] compressedData = compress(originalText);

    var decompressor = new GzipDecompressor();
    var event = decompressor.decompress(new SerializedEvent(null, compressedData, Map.of()));
    var result = new String(event.value(), StandardCharsets.UTF_8);

    assertEquals(originalText, result);
  }

  private byte[] compress(String str) throws IOException {
    var bos = new ByteArrayOutputStream();
    try (var gzip = new GZIPOutputStream(bos)) {
      gzip.write(str.getBytes(StandardCharsets.UTF_8));
    }
    return bos.toByteArray();
  }
}
