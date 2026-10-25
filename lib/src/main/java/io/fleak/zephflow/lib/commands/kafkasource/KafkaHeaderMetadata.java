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
package io.fleak.zephflow.lib.commands.kafkasource;

import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_KAFKA_HEADER_PREFIX;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public final class KafkaHeaderMetadata {

  private KafkaHeaderMetadata() {}

  public static Map<String, String> fromHeaders(ConsumerRecord<byte[], byte[]> record) {
    Map<String, String> metadata = new LinkedHashMap<>();
    for (Header header : record.headers()) {
      if (header.value() == null) {
        continue;
      }
      metadata.put(
          METADATA_KAFKA_HEADER_PREFIX + header.key(),
          new String(header.value(), StandardCharsets.UTF_8));
    }
    return metadata;
  }
}
