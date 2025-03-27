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
package io.fleak.zephflow.lib.serdes.converters;

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import java.util.Map;

/** Created by bolei on 10/16/24 */
public class StringTypedEventConverter extends TypedEventConverter<String> {
  @Override
  protected RecordFleakData payloadToFleakData(String payload) {
    return new RecordFleakData(Map.of(FIELD_NAME_RAW, new StringPrimitiveFleakData(payload)));
  }

  @Override
  public TypedEventContainer<String> fleakDataToTypedEvent(RecordFleakData recordFleakData) {
    // TODO: extract all metadata fields from the event
    throw new UnsupportedOperationException("not yet supported");
  }
}
