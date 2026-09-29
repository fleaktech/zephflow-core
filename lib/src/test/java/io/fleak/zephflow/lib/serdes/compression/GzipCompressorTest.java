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

import static io.fleak.zephflow.lib.utils.CompressionUtils.gunzip;
import static io.fleak.zephflow.lib.utils.CompressionUtils.isGzip;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class GzipCompressorTest {

  private final GzipCompressor compressor = new GzipCompressor();

  @Test
  void compressProducesGzipThatRoundTrips() {
    byte[] original = "{\"a\":1}\n{\"a\":2}\n".getBytes(StandardCharsets.UTF_8);

    byte[] compressed = compressor.compress(original);

    assertTrue(isGzip(compressed));
    assertArrayEquals(original, gunzip(compressed));
  }

  @Test
  void fileExtensionAppendsGz() {
    assertEquals("jsonl.gz", compressor.fileExtension("jsonl"));
  }
}
