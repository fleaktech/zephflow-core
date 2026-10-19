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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.serdes.ser.csv.CsvSerializerOptions;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkHecSinkMessageProcessorTest {

  private RecordFleakData sampleRecord() {
    return new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
  }

  @Test
  void preprocess_allMetadataSet_includesAllKeysAndTrailingNewline() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("security_logs", "paloalto:traffic", "fw01");

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    assertTrue(line.endsWith("\n"), "line must end with newline");

    JsonNode envelope = OBJECT_MAPPER.readTree(line);
    assertTrue(envelope.has("event"));
    assertEquals("hello", envelope.get("event").get("msg").asText());
    assertEquals("security_logs", envelope.get("index").asText());
    assertEquals("paloalto:traffic", envelope.get("sourcetype").asText());
    assertEquals("fw01", envelope.get("source").asText());
  }

  @Test
  void preprocess_allMetadataNull_omitsAllOptionalKeys() throws Exception {
    SplunkHecSinkMessageProcessor processor = new SplunkHecSinkMessageProcessor(null, null, null);

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    JsonNode envelope = OBJECT_MAPPER.readTree(line);
    assertTrue(envelope.has("event"));
    assertFalse(envelope.has("index"));
    assertFalse(envelope.has("sourcetype"));
    assertFalse(envelope.has("source"));
  }

  @Test
  void preprocess_blankMetadataTreatedAsAbsent() throws Exception {
    SplunkHecSinkMessageProcessor processor = new SplunkHecSinkMessageProcessor("", "  ", "");

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertFalse(envelope.has("index"));
    assertFalse(envelope.has("sourcetype"));
    assertFalse(envelope.has("source"));
  }

  @Test
  void preprocess_partialMetadata_includesOnlyNonBlankKeys() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("security_logs", null, null);

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertEquals("security_logs", envelope.get("index").asText());
    assertFalse(envelope.has("sourcetype"));
    assertFalse(envelope.has("source"));
  }

  private RecordFleakData firewallRecord() {
    return new RecordFleakData(
        Map.of(
            "action", new StringPrimitiveFleakData("allow"),
            "src_ip", new StringPrimitiveFleakData("1.2.3.4"),
            "time", new StringPrimitiveFleakData("2026-10-19"),
            "extra", new StringPrimitiveFleakData("dropped")));
  }

  @Test
  void preprocess_csv_eventIsOrderedCsvLineWithoutHeader() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor(
            "security_logs",
            "fleak:csv",
            null,
            new CsvSerializerOptions(List.of("time", "src_ip", "dst_ip", "action"), false));

    SplunkHecOutboundEvent out = processor.preprocess(firewallRecord(), 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    assertTrue(line.endsWith("\n"));
    JsonNode envelope = OBJECT_MAPPER.readTree(line);
    assertTrue(envelope.get("event").isTextual());
    assertEquals("2026-10-19,1.2.3.4,,allow", envelope.get("event").asText());
    assertEquals("security_logs", envelope.get("index").asText());
    assertEquals("fleak:csv", envelope.get("sourcetype").asText());
  }

  @Test
  void preprocess_csv_quotesValuesWithDelimiters() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor(
            null, null, null, new CsvSerializerOptions(List.of("msg", "city"), false));
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "msg", new StringPrimitiveFleakData("hello"),
                "city", new StringPrimitiveFleakData("New, York")));

    SplunkHecOutboundEvent out = processor.preprocess(record, 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertEquals("hello,\"New, York\"", envelope.get("event").asText());
  }

  @Test
  void preprocess_csv_keepsNewlineInsideQuotedValue() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor(
            null, null, null, new CsvSerializerOptions(List.of("msg", "code"), false));
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "msg", new StringPrimitiveFleakData("line1\r\nline2"),
                "code", new StringPrimitiveFleakData("42")));

    SplunkHecOutboundEvent out = processor.preprocess(record, 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    assertEquals(1, line.lines().count(), "envelope must stay on one NDJSON line");
    JsonNode envelope = OBJECT_MAPPER.readTree(line);
    assertEquals("\"line1\r\nline2\",42", envelope.get("event").asText());
  }

  @Test
  void csvOptions_onlyForCsvEncoding() {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder().hecUrl("https://splunk:8088").credentialId("c").build();
    assertNull(SplunkHecSinkMessageProcessor.csvOptions(config));

    config.setEncodingType("JSON_OBJECT");
    config.setCsvColumns(List.of("a"));
    assertNull(SplunkHecSinkMessageProcessor.csvOptions(config));

    config.setEncodingType("CSV");
    CsvSerializerOptions options = SplunkHecSinkMessageProcessor.csvOptions(config);
    assertEquals(List.of("a"), options.columns());
    assertFalse(options.includeHeader());
  }
}
