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
package io.fleak.zephflow.lib.serdes.decode.otlp;

import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures.asPipelineSeesThem;
import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures.json;
import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures.map;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.serdes.decode.config.OtlpFormatConfig.Signal;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpRecordsTest {

  @Test
  void decodesLogsFixture() throws Exception {
    assertEquals(
        OtlpFixtures.logs(),
        asPipelineSeesThem(OtlpRecords.fromJson(Signal.LOGS, json("otlp_logs.json"))));
  }

  @Test
  void decodesMetricsFixture() throws Exception {
    assertEquals(
        OtlpFixtures.metrics(true),
        asPipelineSeesThem(OtlpRecords.fromJson(Signal.METRICS, json("otlp_metrics.json"))));
  }

  @Test
  void decodesTracesFixture() throws Exception {
    assertEquals(
        OtlpFixtures.traces(),
        asPipelineSeesThem(OtlpRecords.fromJson(Signal.TRACES, json("otlp_traces.json"))));
  }

  @Test
  void wrongTypedSchemaUrlIsStringified() {
    Map<String, Object> payload =
        JsonUtils.fromJsonString(
            "{\"resourceLogs\":[{\"schemaUrl\":5,\"scopeLogs\":[{\"logRecords\":[{\"severityText\":\"INFO\"}]}]}]}",
            new TypeReference<>() {});

    assertEquals(
        List.of(map("severityText", "INFO", "resourceSchemaUrl", "5")),
        asPipelineSeesThem(OtlpRecords.fromJson(Signal.LOGS, payload)));
  }

  @Test
  void emptyPayloadHasNoRecords() {
    assertEquals(List.of(), OtlpRecords.fromJson(Signal.METRICS, Map.of()));
  }
}
