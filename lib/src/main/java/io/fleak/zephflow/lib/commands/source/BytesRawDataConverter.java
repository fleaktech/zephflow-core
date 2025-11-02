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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.Decompressor;
import io.fleak.zephflow.lib.serdes.compression.DecompressorFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Created by bolei on 3/26/25 */
@Slf4j
public class BytesRawDataConverter implements RawDataConverter<SerializedEvent> {

  private final FleakDeserializer<?> fleakDeserializer;
  private final Decompressor decompressor;

  public BytesRawDataConverter(FleakDeserializer<?> fleakDeserializer) {
    this(fleakDeserializer, DecompressorFactory.getDecompressor(List.of()));
  }

  public BytesRawDataConverter(FleakDeserializer<?> fleakDeserializer, Decompressor decompressor) {
    this.fleakDeserializer = fleakDeserializer;
    this.decompressor = decompressor;
  }

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.value().length, Map.of());
      List<RecordFleakData> events =
          fleakDeserializer.deserialize(decompressor.decompress(sourceRecord));
      sourceInitializedConfig.inputEventCounter().increase(events.size(), Map.of());
      if (log.isDebugEnabled()) {
        events.forEach(e -> log.debug("got message: {}", toJsonString(e)));
      }
      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("failed to deserialize event:\n{}", sourceRecord);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
