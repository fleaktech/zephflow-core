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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class S3DecodeLimitsTest {

  private static final long MIB = 1024L * 1024;
  private static final long GIB = 1024 * MIB;

  static Stream<Arguments> limits() {
    FormatConfig json = new JsonFormatConfig(JsonFormatConfig.Mode.LINES);
    FormatConfig text = new TextFormatConfig(null, new FramingConfig.Whole());
    FormatConfig avro = new AvroFormatConfig(null, null);
    FormatConfig protobuf = new ProtobufFormatConfig(null, null, null);
    FormatConfig parquet = new ParquetFormatConfig();
    return Stream.of(
        Arguments.of(json, 2 * GIB, new DecodeLimits(16 * MIB, 32 * MIB, 128 * MIB)),
        Arguments.of(text, 2 * GIB, new DecodeLimits(16 * MIB, 32 * MIB, 128 * MIB)),
        Arguments.of(avro, 2 * GIB, new DecodeLimits(16 * MIB, 4 * MIB, 128 * MIB)),
        Arguments.of(protobuf, 2 * GIB, new DecodeLimits(2 * MIB, 2 * MIB, 128 * MIB)),
        Arguments.of(parquet, 2 * GIB, DecodeLimits.UNLIMITED),
        Arguments.of(json, GIB, new DecodeLimits(8 * MIB, 16 * MIB, 128 * MIB)),
        Arguments.of(protobuf, GIB, new DecodeLimits(MIB, MIB, 128 * MIB)),
        Arguments.of(json, 64 * MIB, new DecodeLimits(512 * 1024, MIB, 32 * MIB)));
  }

  @ParameterizedTest
  @MethodSource("limits")
  void limitsAreFixedProportionsOfTheHeap(
      FormatConfig format, long heapBytes, DecodeLimits expected) {
    assertEquals(expected, S3DecodeLimits.forFormat(format, heapBytes));
  }

  @Test
  void theBatchCapIsFourMibOrHeapOver512() {
    assertEquals(
        List.of(4 * MIB, 4 * MIB, 2 * MIB, 128 * 1024L),
        List.of(
            S3DecodeLimits.batchBytes(4 * GIB),
            S3DecodeLimits.batchBytes(2 * GIB),
            S3DecodeLimits.batchBytes(GIB),
            S3DecodeLimits.batchBytes(64 * MIB)));
  }

  @Test
  void taskMaxMemoryWinsWithGridsFloor() {
    assertEquals(
        List.of(2048 * MIB, 1024 * MIB),
        List.of(
            S3DecodeLimits.nominalHeapBytes(
                Map.of("TASK_MAX_MEMORY", "2048"), List.of("-Xmx1g"), GIB - 1),
            S3DecodeLimits.nominalHeapBytes(
                Map.of("TASK_MAX_MEMORY", "768"), List.of("-Xmx1024m"), GIB - 1)));
  }

  @Test
  void withoutTaskMaxMemoryTheLastXmxIsUsedAsGiven() {
    assertEquals(
        List.of(64 * MIB, 2 * GIB, 512 * 1024L, 1_000_000L),
        List.of(
            S3DecodeLimits.nominalHeapBytes(Map.of(), List.of("-Xms8m", "-Xmx64m"), 60 * MIB),
            S3DecodeLimits.nominalHeapBytes(Map.of(), List.of("-Xmx1g", "-Xmx2G"), GIB),
            S3DecodeLimits.nominalHeapBytes(Map.of(), List.of("-Xmx512k"), 1),
            S3DecodeLimits.nominalHeapBytes(Map.of(), List.of("-Xmx1000000"), 1)));
  }

  @Test
  void anUnreadableTaskMaxMemoryOrXmxFallsBack() {
    assertEquals(
        List.of(GIB, 123L),
        List.of(
            S3DecodeLimits.nominalHeapBytes(
                Map.of("TASK_MAX_MEMORY", "lots"), List.of("-Xmx1g"), 123),
            S3DecodeLimits.nominalHeapBytes(Map.of(), List.of("-Xmxbig", "-Xss1m"), 123)));
  }
}
