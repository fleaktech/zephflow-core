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
import java.nio.charset.StandardCharsets;
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
}
