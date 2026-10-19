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
package io.fleak.zephflow.lib.serdes.decode.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.luben.zstd.Zstd;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.compress.Decompressors;
import java.util.List;
import org.junit.jupiter.api.Test;

class AvroBlocksTest {

  /** zstd cannot be made to fail an allocation, so the error code is classified directly. */
  @Test
  void zstdLimitErrorsAreLimitsAndOtherErrorsAreDamage() {
    assertEquals(
        List.of(LimitKind.ZSTD_WINDOW, Decompressors.ZSTD_WINDOW_LIMIT, 0L),
        view(AvroBlocks.zstdTooLarge(Zstd.errFrameParameterWindowTooLarge())));
    assertEquals(
        List.of(LimitKind.NATIVE_MEMORY, 0L, 0L),
        view(AvroBlocks.zstdTooLarge(Zstd.errMemoryAllocation())));
    assertNull(AvroBlocks.zstdTooLarge(Zstd.errChecksumWrong()));
  }

  private static List<Object> view(AvroBlocks.TooLarge tooLarge) {
    return List.of(tooLarge.limit, tooLarge.limitValue, tooLarge.size);
  }
}
