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
import java.util.List;

/**
 * Result of a deserialization that reports failures instead of throwing, so callers can emit the
 * records that did parse and quarantine the ones that didn't.
 *
 * <p>Formats that can only be parsed as a whole document (json array, xml, csv) report a single
 * error covering the entire payload. Line-oriented formats report one error per bad line.
 */
public record DeserializationOutcome(List<RecordFleakData> records, List<RecordError> errors) {

  /**
   * A record that failed to deserialize.
   *
   * @param rawRecord the raw bytes of the failing record, for dead-letter quarantine
   * @param recordIndex 1-based record number within the payload (the line number, for line-oriented
   *     formats), or {@code -1} when the whole payload failed as one unit
   * @param error the failure
   */
  public record RecordError(byte[] rawRecord, int recordIndex, Exception error) {}

  public static DeserializationOutcome success(List<RecordFleakData> records) {
    return new DeserializationOutcome(records, List.of());
  }

  public static DeserializationOutcome wholePayloadFailure(byte[] rawPayload, Exception error) {
    return new DeserializationOutcome(List.of(), List.of(new RecordError(rawPayload, -1, error)));
  }

  public boolean hasErrors() {
    return !errors.isEmpty();
  }
}
