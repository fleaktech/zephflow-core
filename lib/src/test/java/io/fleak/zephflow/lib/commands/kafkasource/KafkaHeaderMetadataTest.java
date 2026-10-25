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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class KafkaHeaderMetadataTest {

  private static ConsumerRecord<byte[], byte[]> recordWith(RecordHeaders headers) {
    return new ConsumerRecord<>(
        "topic", 0, 0L, 0L, null, 0, 0, null, null, headers, Optional.empty());
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void mapsHeadersToPrefixedFields() {
    RecordHeaders headers = new RecordHeaders();
    headers.add("asset_id", utf8("a1"));
    headers.add("source_ip", utf8("10.0.0.1"));

    assertEquals(
        Map.of("__md_kafka_header_asset_id", "a1", "__md_kafka_header_source_ip", "10.0.0.1"),
        KafkaHeaderMetadata.fromHeaders(recordWith(headers)));
  }

  @Test
  void noHeadersGivesEmptyMap() {
    assertEquals(Map.of(), KafkaHeaderMetadata.fromHeaders(recordWith(new RecordHeaders())));
  }

  @Test
  void nullHeaderValueIsSkipped() {
    RecordHeaders headers = new RecordHeaders();
    headers.add("tombstone", null);
    headers.add("asset_id", utf8("a1"));

    assertEquals(
        Map.of("__md_kafka_header_asset_id", "a1"),
        KafkaHeaderMetadata.fromHeaders(recordWith(headers)));
  }

  @Test
  void repeatedHeaderKeyKeepsLastValue() {
    RecordHeaders headers = new RecordHeaders();
    headers.add("asset_id", utf8("first"));
    headers.add("asset_id", utf8("second"));

    assertEquals(
        Map.of("__md_kafka_header_asset_id", "second"),
        KafkaHeaderMetadata.fromHeaders(recordWith(headers)));
  }

  @Test
  void invalidUtf8IsReplacedNotThrown() {
    RecordHeaders headers = new RecordHeaders();
    headers.add("bad", new byte[] {(byte) 0xff});

    assertEquals(
        Map.of("__md_kafka_header_bad", "�"), KafkaHeaderMetadata.fromHeaders(recordWith(headers)));
  }
}
