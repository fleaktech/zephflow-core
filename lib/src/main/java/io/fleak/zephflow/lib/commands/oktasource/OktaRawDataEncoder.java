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
package io.fleak.zephflow.lib.commands.oktasource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OktaRawDataEncoder implements RawDataEncoder<OktaLogEvent> {
  @Override
  public SerializedEvent serialize(OktaLogEvent sourceRecord) {
    try {
      byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(sourceRecord.payload());
      return new SerializedEvent(null, bytes, Map.of());
    } catch (Exception e) {
      log.error("Failed to serialize Okta event: {}", sourceRecord.eventId(), e);
      return new SerializedEvent(null, new byte[0], Map.of());
    }
  }
}
