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

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** The fixed limits of the S3 realtime source, as proportions of a heap size. */
final class S3DecodeLimits {

  private static final long MIB = 1024L * 1024;
  static final String TASK_MAX_MEMORY = "TASK_MAX_MEMORY";
  private static final long GRID_MIN_HEAP_MIB = 1024;

  private S3DecodeLimits() {}

  static DecodeLimits forFormat(FormatConfig format, long heapBytes) {
    long codecMemory = Math.min(128 * MIB, heapBytes / 2);
    return switch (format) {
      case ParquetFormatConfig p -> DecodeLimits.UNLIMITED;
      case AvroFormatConfig a -> new DecodeLimits(heapBytes / 128, heapBytes / 512, codecMemory);
      case ProtobufFormatConfig p ->
          new DecodeLimits(heapBytes / 1024, heapBytes / 1024, codecMemory);
      default -> new DecodeLimits(heapBytes / 128, heapBytes / 64, codecMemory);
    };
  }

  /** The estimated size at which a batch is emitted. */
  static long batchBytes(long heapBytes) {
    return Math.min(4 * MIB, heapBytes / 512);
  }

  /** The heap of this JVM as configured, not as the collector reports it. */
  static long nominalHeapBytes() {
    return nominalHeapBytes(
        System.getenv(),
        ManagementFactory.getRuntimeMXBean().getInputArguments(),
        Runtime.getRuntime().maxMemory());
  }

  /**
   * max({@code TASK_MAX_MEMORY}, 1024) MiB, which is the {@code -Xmx} grid starts a pipeline with;
   * else the last {@code -Xmx} argument; else {@code maxMemory}.
   */
  static long nominalHeapBytes(Map<String, String> env, List<String> jvmArgs, long maxMemory) {
    String task = env.get(TASK_MAX_MEMORY);
    if (task != null) {
      try {
        return Math.max(Long.parseLong(task.trim()), GRID_MIN_HEAP_MIB) * MIB;
      } catch (NumberFormatException e) {
        // grid falls back to its default heap, which is the -Xmx argument below
      }
    }
    Long xmx = null;
    for (String arg : jvmArgs) {
      if (arg.startsWith("-Xmx")) {
        Long parsed = parseSize(arg.substring(4));
        if (parsed != null) {
          xmx = parsed;
        }
      }
    }
    return xmx != null ? xmx : maxMemory;
  }

  private static Long parseSize(String value) {
    if (value.isEmpty()) {
      return null;
    }
    char unit = Character.toLowerCase(value.charAt(value.length() - 1));
    long multiplier =
        switch (unit) {
          case 'k' -> 1024L;
          case 'm' -> MIB;
          case 'g' -> 1024 * MIB;
          case 't' -> 1024 * 1024 * MIB;
          default -> 1;
        };
    String digits = multiplier == 1 ? value : value.substring(0, value.length() - 1);
    try {
      return Long.parseLong(digits.toLowerCase(Locale.ROOT)) * multiplier;
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
