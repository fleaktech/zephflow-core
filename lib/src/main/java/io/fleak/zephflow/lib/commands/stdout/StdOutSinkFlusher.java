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
package io.fleak.zephflow.lib.commands.stdout;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonPayload;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Created by bolei on 12/20/24 */
@Slf4j
public class StdOutSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  private final FleakSerializer<?> fleakSerializer;

  public StdOutSinkFlusher(FleakSerializer<?> fleakSerializer) {
    this.fleakSerializer = fleakSerializer;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
      Map<String, String> metricTags) {
    int successCount = 0;
    long flushedDataSize = 0;
    for (RecordFleakData event : preparedInputEvents.preparedList()) {
      try {
        var serializedEvent = fleakSerializer.serialize(List.of(event));
        System.out.println(new String(serializedEvent.value()));
        successCount++;
        flushedDataSize += serializedEvent.value().length;
      } catch (Exception e) {
        log.error("failed to write to stdout. event: {}", toJsonPayload(event), e);
      }
    }
    return new SimpleSinkCommand.FlushResult(successCount, flushedDataSize, List.of());
  }

  @Override
  public void close() throws IOException {}
}
