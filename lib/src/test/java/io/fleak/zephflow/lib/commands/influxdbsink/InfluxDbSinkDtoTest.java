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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.influxdb.client.domain.WritePrecision;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InfluxDbSinkDtoTest {

  @Test
  void appliesDefaults() {
    InfluxDbSinkDto.Config config =
        InfluxDbSinkDto.Config.builder().url("u").org("o").bucket("b").measurement("m").build();

    assertEquals(WritePrecision.MS, config.getPrecision());
    assertEquals(InfluxDbSinkDto.DEFAULT_BATCH_SIZE, config.getBatchSize());
    assertNotNull(config.getTagFields());
    assertTrue(config.getTagFields().isEmpty());
  }

  @Test
  void parsesFromJsonMap() {
    Map<String, Object> json =
        Map.of(
            "url", "http://localhost:8086",
            "org", "my-org",
            "bucket", "my-bucket",
            "credentialId", "influx-token",
            "measurementField", "event_type",
            "tagFields", List.of("host", "region"),
            "fieldFields", List.of("latency", "status"),
            "timestampField", "ts",
            "precision", "NS",
            "batchSize", 500);

    InfluxDbSinkDto.Config config = OBJECT_MAPPER.convertValue(json, InfluxDbSinkDto.Config.class);

    assertEquals("http://localhost:8086", config.getUrl());
    assertEquals("my-org", config.getOrg());
    assertEquals("my-bucket", config.getBucket());
    assertEquals("influx-token", config.getCredentialId());
    assertEquals("event_type", config.getMeasurementField());
    assertEquals(List.of("host", "region"), config.getTagFields());
    assertEquals(List.of("latency", "status"), config.getFieldFields());
    assertEquals("ts", config.getTimestampField());
    assertEquals(WritePrecision.NS, config.getPrecision()); // enum from string by name
    assertEquals(500, config.getBatchSize());
    assertNull(config.getMeasurement());
  }
}
