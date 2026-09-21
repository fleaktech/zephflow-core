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
package io.fleak.zephflow.lib.serdes.des;

import io.fleak.zephflow.api.structure.RecordFleakData;

/**
 * One ordered decoding outcome. The payload is the original transport bytes, borrowed for the
 * duration of the call; callers must not mutate it. A known raw range refers directly to those
 * bytes. Indices are one-based output/error ordinals; -1 denotes a whole-payload failure or an
 * unknown range. Exactly one of record/error is populated. Exceptions are internal diagnostics, not
 * safe responses.
 */
public record IncrementalRecord(
    RecordFleakData record,
    byte[] rawPayload,
    int subrecordIndex,
    int rawOffset,
    int rawLength,
    Exception error) {}
