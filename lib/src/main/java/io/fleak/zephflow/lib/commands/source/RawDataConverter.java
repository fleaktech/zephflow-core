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
package io.fleak.zephflow.lib.commands.source;

/** Created by bolei on 3/26/25 */
// converts raw data from an external source into internal data structure
public interface RawDataConverter<T> {
  /**
   * Transforms source record to internal format
   *
   * @param sourceRecord The original record from the source
   * @return Transformed result containing either success with data or failure with original record
   */
  ConvertedResult<T> convert(T sourceRecord);
}
