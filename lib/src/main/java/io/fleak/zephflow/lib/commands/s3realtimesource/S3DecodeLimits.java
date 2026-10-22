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

import io.fleak.zephflow.lib.commands.filedecode.HeapDecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import java.util.List;
import java.util.Map;

/** The fixed limits of the S3 realtime source, as proportions of a heap size. */
final class S3DecodeLimits {

  private S3DecodeLimits() {}

  static DecodeLimits forFormat(FormatConfig format, long heapBytes) {
    return HeapDecodeLimits.forFormat(format, heapBytes);
  }

  static long batchBytes(long heapBytes) {
    return HeapDecodeLimits.batchBytes(heapBytes);
  }

  static long nominalHeapBytes() {
    return HeapDecodeLimits.nominalHeapBytes();
  }

  static long nominalHeapBytes(Map<String, String> env, List<String> jvmArgs, long maxMemory) {
    return HeapDecodeLimits.nominalHeapBytes(env, jvmArgs, maxMemory);
  }
}
