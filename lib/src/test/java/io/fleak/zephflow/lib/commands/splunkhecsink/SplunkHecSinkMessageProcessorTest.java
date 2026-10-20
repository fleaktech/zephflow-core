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
import io.fleak.zephflow.lib.serdes.ser.kv.KvSerializerOptions;
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
        csvProcessor(
            "security_logs", "fleak:csv", null, List.of("time", "src_ip", "dst_ip", "action"));

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
        csvProcessor(null, null, null, List.of("msg", "city"));
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
        csvProcessor(null, null, null, List.of("msg", "code"));
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

  @Test
  void preprocess_kv_eventIsKeyValueLine() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        kvProcessor(
            "security_logs", "fleak:kv", null, List.of("time", "src_ip", "dst_ip", "action"));

    SplunkHecOutboundEvent out = processor.preprocess(firewallRecord(), 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertEquals("time=2026-10-19 src_ip=1.2.3.4 action=allow", envelope.get("event").asText());
    assertEquals("fleak:kv", envelope.get("sourcetype").asText());
  }

  @Test
  void preprocess_kv_rejectsEventWithNoConfiguredFields() {
    SplunkHecSinkMessageProcessor processor = kvProcessor(null, null, null, List.of("missing"));

    assertThrows(IllegalArgumentException.class, () -> processor.preprocess(firewallRecord(), 0L));
  }

  @Test
  void kvOptions_onlyForKvEncoding() {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder().hecUrl("https://splunk:8088").credentialId("c").build();
    config.setKvFields(List.of("a"));
    config.setKvPairDelimiter("=");
    assertFalse(config.usesKvEncoding());
    assertEquals(KvSerializerOptions.DEFAULT, config.kvOptions());

    config.setEncodingType("KV");
    config.setKvPairDelimiter("|");
    config.setKvCleanFields(true);
    KvSerializerOptions options = config.kvOptions();
    assertEquals(List.of("a"), options.fields());
    assertEquals("|", options.pairDelimiter());
    assertTrue(options.cleanFields());
  }

  private static String eventOf(SplunkHecSinkMessageProcessor processor, RecordFleakData record)
      throws Exception {
    return OBJECT_MAPPER
        .readTree(
            new String(
                processor.preprocess(record, 0L).preEncodedNdjsonLine(), StandardCharsets.UTF_8))
        .get("event")
        .toString();
  }

  @Test
  void fromConfig_picksSerializerFromEncodingType() throws Exception {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder().hecUrl("https://splunk:8088").credentialId("c").build();
    config.setCsvColumns(List.of("src_ip", "action"));
    config.setKvFields(List.of("src_ip", "action"));
    RecordFleakData record = firewallRecord();

    assertTrue(eventOf(SplunkHecSinkMessageProcessor.fromConfig(config), record).startsWith("{"));

    config.setEncodingType("CSV");
    assertEquals(
        "\"1.2.3.4,allow\"", eventOf(SplunkHecSinkMessageProcessor.fromConfig(config), record));

    config.setEncodingType("KV");
    assertEquals(
        "\"src_ip=1.2.3.4 action=allow\"",
        eventOf(SplunkHecSinkMessageProcessor.fromConfig(config), record));
  }

  @Test
  void preprocess_kv_quotedValueStaysOneJsonEscapedLine() throws Exception {
    SplunkHecSinkMessageProcessor processor = kvProcessor(null, null, null, List.of("msg"));
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("say \"hi\"\nbye")));

    SplunkHecOutboundEvent out = processor.preprocess(record, 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    assertEquals(1, line.lines().count(), "envelope must stay on one NDJSON line");
    assertEquals(
        "msg=\"say \\\"hi\\\"\\nbye\"", OBJECT_MAPPER.readTree(line).get("event").asText());
  }

  @Test
  void preprocess_kv_nonAsciiSurvivesAsUtf8() throws Exception {
    SplunkHecSinkMessageProcessor processor = kvProcessor(null, null, null, List.of("city"));
    RecordFleakData record =
        new RecordFleakData(Map.of("city", new StringPrimitiveFleakData("Zürich 苏黎世")));

    SplunkHecOutboundEvent out = processor.preprocess(record, 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertEquals("city=\"Zürich 苏黎世\"", envelope.get("event").asText());
  }

  private static SplunkHecSinkMessageProcessor csvProcessor(
      String index, String sourcetype, String source, List<String> columns) {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder()
            .hecUrl("https://splunk:8088")
            .credentialId("c")
            .index(index)
            .sourcetype(sourcetype)
            .source(source)
            .encodingType("CSV")
            .csvColumns(columns)
            .build();
    return SplunkHecSinkMessageProcessor.fromConfig(config);
  }

  private static SplunkHecSinkMessageProcessor kvProcessor(
      String index, String sourcetype, String source, List<String> fields) {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder()
            .hecUrl("https://splunk:8088")
            .credentialId("c")
            .index(index)
            .sourcetype(sourcetype)
            .source(source)
            .encodingType("KV")
            .kvFields(fields)
            .build();
    return SplunkHecSinkMessageProcessor.fromConfig(config);
  }
}
