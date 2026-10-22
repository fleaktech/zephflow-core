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

import static io.fleak.zephflow.lib.commands.azureiothubsource.IotHubRawDataConverter.ENVELOPE_KEY;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.Map;
import lombok.RequiredArgsConstructor;

/**
 * Decodes IoT Hub events with a {@link MessageDecodingConverter} and merges the {@code _iothub}
 * envelope into every record, as {@link IotHubRawDataConverter} does.
 */
@RequiredArgsConstructor
public class IotHubMessageConverter implements RawDataConverter<SerializedEvent> {

  private final MessageDecodingConverter decoding;

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> context) {
    ConvertedResult<SerializedEvent> result = decoding.convert(sourceRecord, context);
    if (result.error() != null) {
      return result;
    }
    FleakData envelope = IotHubRawDataConverter.buildEnvelope(sourceRecord.metadata());
    if (envelope == null) {
      return result;
    }
    return ConvertedResult.success(
        result.transformedData().stream()
            .map(r -> r.copyAndMerge(Map.of(ENVELOPE_KEY, envelope)))
            .toList(),
        sourceRecord);
  }
}
