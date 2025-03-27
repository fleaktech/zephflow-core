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

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;

/** Created by bolei on 3/26/25 */
@Getter
public class ConvertedResult<T> {
  private final List<RecordFleakData> transformedData;
  private final Exception error;
  @NonNull private final T sourceRecord;

  private ConvertedResult(
      List<RecordFleakData> transformedData, Exception error, @NonNull T sourceRecord) {
    this.transformedData = transformedData;
    this.error = error;
    this.sourceRecord = sourceRecord;
  }

  public static <T> ConvertedResult<T> success(
      List<RecordFleakData> data, @NonNull T sourceRecord) {
    return new ConvertedResult<>(data, null, sourceRecord);
  }

  public static <T> ConvertedResult<T> failure(Exception error, @NonNull T sourceRecord) {
    return new ConvertedResult<>(null, error, sourceRecord);
  }
}
