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
package io.fleak.zephflow.lib.serdes.decode.compress;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

class ZstdFrameStreamTest {

  /** zstd cannot be made to fail an allocation, so the failure is handed to the classification. */
  @Test
  void anAllocationFailureIsOverNativeMemory() throws Exception {
    DecodeFlags flags = new DecodeFlags();
    try (ZstdFrameStream stream = new ZstdFrameStream(InputStream.nullInputStream(), flags)) {
      stream.classify(new ZstdException(Zstd.errMemoryAllocation(), "Allocation error"));
    }
    DecodeFlags.BudgetExceeded exceeded = flags.getBudgetExceeded();
    assertEquals(0, exceeded.preview().length);
    assertEquals(
        new DecodeFlags.BudgetExceeded(-1, LimitKind.NATIVE_MEMORY, 0, 0, exceeded.preview()),
        exceeded);
  }
}
