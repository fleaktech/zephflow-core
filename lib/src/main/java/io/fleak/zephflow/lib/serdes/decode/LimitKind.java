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
package io.fleak.zephflow.lib.serdes.decode;

/**
 * The limit an {@link DecodeOutcome.Oversized} went over, named by what raises it. Its {@code
 * limitValue} is the limit that applied, in bytes unless stated otherwise; for a parquet row
 * without {@code maxRecordBytes} it is 1/64 of the parquet budget the kind names (3.4 of the
 * generic decoder spec).
 */
public enum LimitKind {
  /** {@link DecodeLimits#maxRecordBytes()}. */
  MAX_RECORD_BYTES,
  /** {@link DecodeLimits#maxDocumentBytes()}. */
  MAX_DOCUMENT_BYTES,
  /**
   * The memory xz may use: {@link DecodeLimits#maxCodecMemoryBytes()}, else {@link
   * DecodeLimits#maxDocumentBytes()}, else 128 MiB or half the heap, whichever is smaller (inside
   * an avro block always the last).
   */
  CODEC_MEMORY,
  /** Parquet's own share of the heap, used where the caller left a limit at 0. */
  HEAP_SHARE,
  /** A multiline rule's {@code maxLines}; the value is a number of lines. */
  MAX_LINES,
  /** zstd's window limit, fixed at 128 MiB by zstd-jni. */
  ZSTD_WINDOW,
  /** A codec could not allocate native memory; the value is 0. */
  NATIVE_MEMORY,
  /**
   * The most one unit can hold in a Java array, about 2 GiB (an avro block, a protobuf message).
   */
  FORMAT_MAXIMUM
}
