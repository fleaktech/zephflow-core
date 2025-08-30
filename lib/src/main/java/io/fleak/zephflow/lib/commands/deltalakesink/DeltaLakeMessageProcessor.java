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
package io.fleak.zephflow.lib.commands.deltalakesink;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeltaLakeMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> {

  @Override
  public Map<String, Object> preprocess(RecordFleakData event, long ts) {
    Map<String, Object> unwrapped = event.unwrap();

    // Create mutable copy since unwrap() may return immutable map
    Map<String, Object> result = new HashMap<>(unwrapped);
    result.put("_fleak_timestamp", ts);

    return result;
  }
}
