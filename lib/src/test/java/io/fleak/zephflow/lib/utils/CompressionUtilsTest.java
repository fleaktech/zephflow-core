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
package io.fleak.zephflow.lib.utils;

import static io.fleak.zephflow.lib.utils.CompressionUtils.gunzip;
import static io.fleak.zephflow.lib.utils.CompressionUtils.gzip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class CompressionUtilsTest {

  private static final byte[] PAYLOAD = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);

  @Test
  void detectsGzipByItsMagicBytes() {
    assertEquals(
        List.of(false, false, false, false, true),
        Stream.of(null, new byte[0], new byte[] {0x1f}, PAYLOAD, gzip(PAYLOAD))
            .map(CompressionUtils::isGzip)
            .toList());
  }

  @Test
  void gunzipRoundTripsAGzipPayload() {
    assertEquals(
        new String(PAYLOAD, StandardCharsets.UTF_8),
        new String(gunzip(gzip(PAYLOAD)), StandardCharsets.UTF_8));
  }

  @Test
  void gunzipRejectsATruncatedGzipPayload() {
    byte[] compressed = gzip(PAYLOAD);
    byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);

    assertThrows(IOException.class, () -> gunzip(truncated));
  }
}
