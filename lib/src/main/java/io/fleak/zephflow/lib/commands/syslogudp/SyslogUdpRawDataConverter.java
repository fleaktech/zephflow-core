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
package io.fleak.zephflow.lib.commands.syslogudp;

import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SyslogUdpRawDataConverter implements RawDataConverter<SerializedEvent> {

  private final Charset charset;

  public SyslogUdpRawDataConverter(Charset charset) {
    this.charset = charset;
  }

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> sourceExecutionContext) {
    try {
      String message = new String(sourceRecord.value(), charset);
      Map<String, String> metadata = sourceRecord.metadata();

      String sourceAddress = metadata.get(SyslogUdpDto.METADATA_SOURCE_ADDRESS);
      int sourcePort = Integer.parseInt(metadata.get(SyslogUdpDto.METADATA_SOURCE_PORT));
      long receivedAt = Long.parseLong(metadata.get(SyslogUdpDto.METADATA_RECEIVED_AT));

      RecordFleakData record =
          new RecordFleakData(
              Map.of(
                  "message",
                  new StringPrimitiveFleakData(message),
                  "source_address",
                  new StringPrimitiveFleakData(sourceAddress),
                  "source_port",
                  new NumberPrimitiveFleakData(
                      sourcePort, NumberPrimitiveFleakData.NumberType.LONG),
                  "received_at",
                  new NumberPrimitiveFleakData(
                      receivedAt, NumberPrimitiveFleakData.NumberType.LONG)));

      sourceExecutionContext.dataSizeCounter().increase(sourceRecord.value().length, Map.of());
      sourceExecutionContext.inputEventCounter().increase(1, Map.of());

      return ConvertedResult.success(List.of(record), sourceRecord);
    } catch (Exception e) {
      sourceExecutionContext.deserializeFailureCounter().increase(Map.of());
      log.error("failed to convert UDP datagram", e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
