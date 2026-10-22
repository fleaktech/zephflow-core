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

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** The fixed decoding limits of the file sources, as proportions of a heap size. */
public final class HeapDecodeLimits {

  private static final long MIB = 1024L * 1024;
  private static final String TASK_MAX_MEMORY = "TASK_MAX_MEMORY";
  private static final String MAX_HEAP_SIZE = "-XX:MaxHeapSize=";
  private static final long GRID_MIN_HEAP_MIB = 1024;

  private HeapDecodeLimits() {}

  public static DecodeLimits forFormat(FormatConfig format, long heapBytes) {
    long codecMemory = codecMemory(heapBytes);
    return switch (format) {
      case ParquetFormatConfig p -> DecodeLimits.UNLIMITED;
      case AvroFormatConfig a -> new DecodeLimits(heapBytes / 128, heapBytes / 512, codecMemory);
      case ProtobufFormatConfig p ->
          new DecodeLimits(heapBytes / 1024, heapBytes / 1024, codecMemory);
      default -> new DecodeLimits(heapBytes / 128, heapBytes / 64, codecMemory);
    };
  }

  /**
   * The limits with {@code configuredBytes} as both the record and the document limit of every
   * format, or the heap proportions when it is null.
   */
  public static DecodeLimits forFormat(FormatConfig format, long heapBytes, Long configuredBytes) {
    return configuredBytes == null
        ? forFormat(format, heapBytes)
        : new DecodeLimits(configuredBytes, configuredBytes, codecMemory(heapBytes));
  }

  private static long codecMemory(long heapBytes) {
    return Math.min(128 * MIB, heapBytes / 2);
  }

  /** The estimated size at which a batch is emitted. */
  public static long batchBytes(long heapBytes) {
    return Math.min(4 * MIB, heapBytes / 512);
  }

  /** The heap of this JVM as configured, not as the collector reports it. */
  public static long nominalHeapBytes() {
    return nominalHeapBytes(
        System.getenv(),
        ManagementFactory.getRuntimeMXBean().getInputArguments(),
        Runtime.getRuntime().maxMemory());
  }

  /**
   * The last {@code -Xmx} or {@code -XX:MaxHeapSize=} argument, which is the heap the JVM was
   * started with; else max({@code TASK_MAX_MEMORY}, 1024) MiB, which is the {@code -Xmx} grid
   * starts a pipeline with, parsed as grid parses it; else {@code maxMemory}.
   */
  public static long nominalHeapBytes(
      Map<String, String> env, List<String> jvmArgs, long maxMemory) {
    Long xmx = null;
    for (String arg : jvmArgs) {
      String size =
          arg.startsWith("-Xmx")
              ? arg.substring(4)
              : arg.startsWith(MAX_HEAP_SIZE) ? arg.substring(MAX_HEAP_SIZE.length()) : null;
      if (size != null) {
        Long parsed = parseSize(size);
        if (parsed != null) {
          xmx = parsed;
        }
      }
    }
    if (xmx != null) {
      return xmx;
    }
    String task = env.get(TASK_MAX_MEMORY);
    if (task != null) {
      try {
        return Math.max(Integer.parseInt(task), GRID_MIN_HEAP_MIB) * MIB;
      } catch (NumberFormatException e) {
        return GRID_MIN_HEAP_MIB * MIB;
      }
    }
    return maxMemory;
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
