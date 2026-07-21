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
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.Decompressor;
import io.fleak.zephflow.lib.serdes.compression.DecompressorFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Deserializes an IoT Hub telemetry message body and nests IoT metadata (device id, enqueued time,
 * application properties) under a reserved {@code _iothub} envelope key, leaving the body's own
 * fields at the top level. Metadata is carried on {@link SerializedEvent#metadata()} as a flat map
 * (see {@link IotHubMetadataExtractor}); application properties arrive prefixed with {@link
 * #PROPERTY_PREFIX} and are re-nested under {@code properties} here.
 */
@Slf4j
public class IotHubRawDataConverter implements RawDataConverter<SerializedEvent> {

  public static final String ENVELOPE_KEY = "_iothub";
  public static final String DEVICE_ID_KEY = "deviceId";
  public static final String ENQUEUED_TIME_KEY = "enqueuedTime";
  public static final String PROPERTIES_KEY = "properties";
  public static final String PROPERTY_PREFIX = "prop.";

  private final FleakDeserializer<?> fleakDeserializer;
  private final Decompressor decompressor;

  public IotHubRawDataConverter(FleakDeserializer<?> fleakDeserializer) {
    this(fleakDeserializer, DecompressorFactory.getDecompressor(List.of()));
  }

  public IotHubRawDataConverter(FleakDeserializer<?> fleakDeserializer, Decompressor decompressor) {
    this.fleakDeserializer = fleakDeserializer;
    this.decompressor = decompressor;
  }

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      List<RecordFleakData> deserialized =
          fleakDeserializer.deserialize(decompressor.decompress(sourceRecord));

      FleakData envelope = buildEnvelope(sourceRecord.metadata());
      List<RecordFleakData> events =
          envelope == null
              ? deserialized
              : deserialized.stream()
                  .map(r -> r.copyAndMerge(Map.of(ENVELOPE_KEY, envelope)))
                  .toList();

      Map<String, String> eventTags =
          getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());
      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.value().length, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);
      if (log.isDebugEnabled()) {
        events.forEach(e -> log.debug("got message: {}", toJsonString(e)));
      }
      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.value().length, Map.of());
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("failed to deserialize IoT Hub event:\n{}", sourceRecord);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }

  private static FleakData buildEnvelope(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    Map<String, Object> envelope = new LinkedHashMap<>();
    Map<String, Object> properties = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : metadata.entrySet()) {
      if (e.getKey().startsWith(PROPERTY_PREFIX)) {
        properties.put(e.getKey().substring(PROPERTY_PREFIX.length()), e.getValue());
      } else {
        envelope.put(e.getKey(), e.getValue());
      }
    }
    if (!properties.isEmpty()) {
      envelope.put(PROPERTIES_KEY, properties);
    }
    return FleakData.wrap(envelope);
  }
}
