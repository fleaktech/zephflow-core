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

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.format;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
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
  void fromConfig_picksTheEncoder() throws Exception {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder().hecUrl("https://splunk:8088").credentialId("c").build();
    RecordFleakData record = firewallRecord();

    assertTrue(eventOf(SplunkHecSinkMessageProcessor.fromConfig(config), record).startsWith("{"));

    config.setEncoding(
        format("{\"type\":\"csv\",\"header\":false,\"columns\":[\"src_ip\",\"action\"]}"));
    assertEquals(
        "\"1.2.3.4,allow\"", eventOf(SplunkHecSinkMessageProcessor.fromConfig(config), record));

    config.setEncoding(format("{\"type\":\"kv\",\"fields\":[\"src_ip\",\"action\"]}"));
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
            .encoding(
                format(
                    "{\"type\":\"csv\",\"header\":false,\"columns\":"
                        + JsonUtils.toJsonString(columns)
                        + "}"))
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
            .encoding(format("{\"type\":\"kv\",\"fields\":" + JsonUtils.toJsonString(fields) + "}"))
            .build();
    return SplunkHecSinkMessageProcessor.fromConfig(config);
  }

  private static SplunkHecSinkMessageProcessor textProcessor(String framing) {
    SplunkHecSinkDto.Config config =
        SplunkHecSinkDto.Config.builder()
            .hecUrl("https://splunk:8088")
            .credentialId("c")
            .index("netfw")
            .sourcetype("cisco:asa")
            .encoding(format("{\"type\":\"text\",\"framing\":" + framing + "}"))
            .build();
    return SplunkHecSinkMessageProcessor.fromConfig(config);
  }

  private static RecordFleakData raw(String text) {
    return new RecordFleakData(Map.of("__raw__", new StringPrimitiveFleakData(text)));
  }

  private static String ndjson(SplunkHecSinkMessageProcessor processor, RecordFleakData record)
      throws Exception {
    return new String(
        processor.preprocess(record, 0L).preEncodedNdjsonLine(), StandardCharsets.UTF_8);
  }

  @Test
  void preprocess_text_eventIsTheRawLineUnchanged() throws Exception {
    assertEquals(
        "{\"event\":\"%ASA-6-302013: Built inbound TCP connection 1 for outside:10.0.0.1/443\","
            + "\"sourcetype\":\"cisco:asa\",\"index\":\"netfw\"}\n",
        ndjson(
            textProcessor("{\"type\":\"line\"}"),
            raw("%ASA-6-302013: Built inbound TCP connection 1 for outside:10.0.0.1/443")));
  }

  @Test
  void preprocess_text_keepsEveryCrlfIncludingTheTrailingOne() throws Exception {
    assertEquals(
        "{\"event\":\"<Event>\\r\\n  <System/>\\r\\n</Event>\\r\\n\","
            + "\"sourcetype\":\"cisco:asa\",\"index\":\"netfw\"}\n",
        ndjson(
            textProcessor("{\"type\":\"multiline\",\"startPattern\":\"^<Event\"}"),
            raw("<Event>\r\n  <System/>\r\n</Event>\r\n")));
  }

  private static FleakData str(String value) {
    return new StringPrimitiveFleakData(value);
  }

  private static JsonNode envelopeOf(
      SplunkHecSinkMessageProcessor processor, RecordFleakData record) throws Exception {
    return OBJECT_MAPPER.readTree(ndjson(processor, record));
  }

  private static JsonNode json(String text) throws Exception {
    return OBJECT_MAPPER.readTree(text);
  }

  @Test
  void preprocess_recordLabelsWinAndConfigFillsTheRest() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("cfg_idx", "cfg:st", null);
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "msg", str("hi"),
                "__host__", str("web01"),
                "__sourcetype__", str("app:log"),
                "__ts__", new NumberPrimitiveFleakData(1730800000123L, NumberType.LONG)));

    assertEquals(
        json(
            "{\"event\":{\"msg\":\"hi\"},\"host\":\"web01\",\"sourcetype\":\"app:log\","
                + "\"index\":\"cfg_idx\",\"time\":1730800000.123}"),
        envelopeOf(processor, record));
  }

  @Test
  void preprocess_everyRecordLabelIsSentAndStripped() throws Exception {
    SplunkHecSinkMessageProcessor processor = new SplunkHecSinkMessageProcessor(null, null, null);
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "msg", str("hi"),
                "__host__", str("web01"),
                "__source__", str("/var/log/a"),
                "__sourcetype__", str("app:log"),
                "__index__", str("main"),
                "__md_kafka_topic", str("t")));

    assertEquals(
        json(
            "{\"event\":{\"msg\":\"hi\",\"__md_kafka_topic\":\"t\"},\"host\":\"web01\","
                + "\"sourcetype\":\"app:log\",\"index\":\"main\",\"source\":\"/var/log/a\"}"),
        envelopeOf(processor, record));
  }

  @Test
  void preprocess_stringTimestampIsNotSentAndStaysInTheEvent() throws Exception {
    SplunkHecSinkMessageProcessor processor = new SplunkHecSinkMessageProcessor(null, null, null);
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", str("hi"), "__ts__", str("6/3/2025 7:24:49 AM")));

    assertEquals(
        json("{\"event\":{\"msg\":\"hi\",\"__ts__\":\"6/3/2025 7:24:49 AM\"}}"),
        envelopeOf(processor, record));
  }

  @Test
  void preprocess_blankOrNonStringLabelFallsBackToConfig() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("cfg_idx", null, null);
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "msg", str("hi"),
                "__host__", str("  "),
                "__index__", new NumberPrimitiveFleakData(7, NumberType.LONG)));

    assertEquals(
        json("{\"event\":{\"msg\":\"hi\"},\"index\":\"cfg_idx\"}"), envelopeOf(processor, record));
  }

  @Test
  void preprocess_timeIsSecondsWithMillisecondPrecision() throws Exception {
    SplunkHecSinkMessageProcessor processor = new SplunkHecSinkMessageProcessor(null, null, null);

    assertEquals(
        json("{\"event\":{\"msg\":\"hi\"},\"time\":1730800000.123}"),
        envelopeOf(
            processor,
            new RecordFleakData(
                Map.of(
                    "msg",
                    str("hi"),
                    "__ts__",
                    new NumberPrimitiveFleakData(1730800000123.9, NumberType.DOUBLE)))));
    JsonNode wholeSecond =
        envelopeOf(
            processor,
            new RecordFleakData(
                Map.of(
                    "msg",
                    str("hi"),
                    "__ts__",
                    new NumberPrimitiveFleakData(1730800000000L, NumberType.LONG))));
    assertEquals(0, new BigDecimal("1730800000").compareTo(wholeSecond.get("time").decimalValue()));
  }

  @Test
  void preprocess_labelValuesNeverReachLineEncodings() throws Exception {
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "__raw__", str("raw line"),
                "action", str("allow"),
                "__host__", str("web01"),
                "__ts__", new NumberPrimitiveFleakData(1730800000123L, NumberType.LONG)));

    assertEquals(
        json("{\"event\":\",allow\",\"host\":\"web01\",\"time\":1730800000.123}"),
        envelopeOf(csvProcessor(null, null, null, List.of("__host__", "action")), record));
    assertEquals(
        json("{\"event\":\"action=allow\",\"host\":\"web01\",\"time\":1730800000.123}"),
        envelopeOf(kvProcessor(null, null, null, List.of("__host__", "action")), record));
    assertEquals(
        json(
            "{\"event\":\"raw line\",\"host\":\"web01\",\"sourcetype\":\"cisco:asa\","
                + "\"index\":\"netfw\",\"time\":1730800000.123}"),
        envelopeOf(textProcessor("{\"type\":\"line\"}"), record));
  }

  @Test
  void preprocess_leavesTheInputRecordUnchanged() throws Exception {
    Map<String, FleakData> payload =
        Map.of(
            "msg", str("hi"),
            "__host__", str("web01"),
            "__ts__", new NumberPrimitiveFleakData(1730800000123L, NumberType.LONG));
    RecordFleakData record = new RecordFleakData(new HashMap<>(payload));

    new SplunkHecSinkMessageProcessor(null, null, null).preprocess(record, 0L);

    assertEquals(new RecordFleakData(payload), record);
  }

  @Test
  void preprocess_text_refusesARecordWithoutRawText() {
    SplunkHecSinkMessageProcessor processor = textProcessor("{\"type\":\"line\"}");
    assertEquals(
        "record has no string __raw__ field to write as text",
        assertThrows(
                IllegalArgumentException.class, () -> processor.preprocess(firewallRecord(), 0L))
            .getMessage());
  }
}
