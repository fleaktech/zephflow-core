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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;

/**
 * Decodes IoT Hub events with a {@link MessageDecodingConverter} and nests IoT metadata (device id,
 * enqueued time, application properties) under a reserved {@code _iothub} envelope key in every
 * record, leaving the body's own fields at the top level. Metadata is carried on {@link
 * SerializedEvent#metadata()} as a flat map (see {@link IotHubMetadataExtractor}); application
 * properties arrive prefixed with {@link #PROPERTY_PREFIX} and are re-nested under {@code
 * properties} here.
 */
@RequiredArgsConstructor
public class IotHubMessageConverter implements RawDataConverter<SerializedEvent> {

  public static final String ENVELOPE_KEY = "_iothub";
  public static final String DEVICE_ID_KEY = "deviceId";
  public static final String ENQUEUED_TIME_KEY = "enqueuedTime";
  public static final String PROPERTIES_KEY = "properties";
  public static final String PROPERTY_PREFIX = "prop.";

  private final MessageDecodingConverter decoding;

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> context) {
    ConvertedResult<SerializedEvent> result = decoding.convert(sourceRecord, context);
    if (result.error() != null) {
      return result;
    }
    FleakData envelope = buildEnvelope(sourceRecord.metadata());
    if (envelope == null) {
      return result;
    }
    return ConvertedResult.success(
        result.transformedData().stream()
            .map(r -> r.copyAndMerge(Map.of(ENVELOPE_KEY, envelope)))
            .toList(),
        sourceRecord);
  }

  static FleakData buildEnvelope(Map<String, String> metadata) {
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
    // Always surface {@code properties} (empty object when the message carried none) so downstream
    // consumers can rely on {@code _iothub.properties} being present whenever the envelope exists.
    envelope.put(PROPERTIES_KEY, properties);
    return FleakData.wrap(envelope);
  }
}
