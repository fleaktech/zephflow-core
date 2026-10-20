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
package io.fleak.zephflow.lib.commands.filedecode;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The configured override; the heap proportions are tested through the S3 realtime source. */
class HeapDecodeLimitsTest {

  private static final long MIB = 1024L * 1024;
  private static final List<FormatConfig> FORMATS =
      List.of(
          new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT),
          new AvroFormatConfig(null, null),
          new ProtobufFormatConfig(null, null, null),
          new ParquetFormatConfig());

  @Test
  void aConfiguredLimitIsTheRecordAndDocumentLimitOfEveryFormat() {
    assertEquals(
        List.of(
            new DecodeLimits(300 * MIB, 300 * MIB, 128 * MIB),
            new DecodeLimits(300 * MIB, 300 * MIB, 128 * MIB),
            new DecodeLimits(300 * MIB, 300 * MIB, 128 * MIB),
            new DecodeLimits(300 * MIB, 300 * MIB, 128 * MIB)),
        FORMATS.stream().map(f -> HeapDecodeLimits.forFormat(f, 2048 * MIB, 300 * MIB)).toList());
  }

  @Test
  void withoutAConfiguredLimitTheHeapDecides() {
    assertEquals(
        FORMATS.stream().map(f -> HeapDecodeLimits.forFormat(f, 64 * MIB)).toList(),
        FORMATS.stream().map(f -> HeapDecodeLimits.forFormat(f, 64 * MIB, null)).toList());
  }

  @Test
  void theCodecMemoryStillComesFromTheHeap() {
    assertEquals(
        new DecodeLimits(MIB, MIB, 32 * MIB),
        HeapDecodeLimits.forFormat(
            new JsonFormatConfig(JsonFormatConfig.Mode.LINES), 64 * MIB, MIB));
  }
}
