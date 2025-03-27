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
package io.fleak.zephflow.sdk;

import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionConfig.ComponentType.*;
import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.parser.extractions.GrokExtractionConfig;
import io.fleak.zephflow.lib.parser.extractions.SyslogExtractionConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/18/25 */
public class ZephFlowParserNodeTest {

  private InputStream in;
  private ByteArrayOutputStream testOut;
  private PrintStream psOut;

  @BeforeEach
  public void setup() {
    String rawLogStr =
        "Oct 10 2018 12:34:56 localhost CiscoASA[999]: %ASA-6-305011: Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256";
    in = new ByteArrayInputStream(Objects.requireNonNull(rawLogStr).getBytes());
    testOut = new ByteArrayOutputStream();
    psOut = new PrintStream(testOut);
    System.setIn(in);
    System.setOut(psOut);
  }

  @AfterEach
  public void teardown() {
    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    psOut.close();
  }

  @Test
  public void testParserNode() throws Exception {

    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField(FIELD_NAME_RAW)
            .extractionConfig(
                SyslogExtractionConfig.builder()
                    .timestampPattern("MMM dd yyyy HH:mm:ss")
                    .componentList(List.of(TIMESTAMP, DEVICE, APP))
                    .messageBodyDelimiter(':')
                    .build())
            .dispatchConfig(
                ParserConfigs.DispatchConfig.builder()
                    .dispatchField("content")
                    .defaultConfig(
                        ParserConfigs.ParserConfig.builder()
                            .targetField("content")
                            .removeTargetField(true)
                            .extractionConfig(
                                GrokExtractionConfig.builder()
                                    .grokExpression(
                                        "%ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
                                    .build())
                            .dispatchConfig(
                                ParserConfigs.DispatchConfig.builder()
                                    .dispatchField("message_number")
                                    .dispatchMap(
                                        Map.of(
                                            "305011",
                                            ParserConfigs.ParserConfig.builder()
                                                .targetField("message_text")
                                                .removeTargetField(true)
                                                .extractionConfig(
                                                    GrokExtractionConfig.builder()
                                                        .grokExpression(
                                                            "%{WORD:action} %{WORD:translation_type} %{WORD:protocol} translation from %{WORD:source_interface}:%{IP:source_ip}/%{INT:source_port} to %{WORD:dest_interface}:%{IP:dest_ip}/%{INT:dest_port}")
                                                        .build())
                                                .build()))
                                    .build())
                            .build())
                    .build())
            .build();

    var flow =
        ZephFlow.startFlow()
            .stdinSource(EncodingType.STRING_LINE)
            .parse(parserConfig)
            .stdoutSink(EncodingType.JSON_OBJECT);
    flow.execute("test_id", "test_env", "test_service");
    String output = testOut.toString();
    List<String> lines = output.lines().toList();
    var objects =
        lines.subList(1, lines.size()).stream()
            .map(l -> fromJsonString(l, new TypeReference<Map<String, Object>>() {}))
            .toList();
    assertEquals(1, objects.size());

    var parsedStr =
        """
  {
    "level": "6",
    "message_number": "305011",
    "source_interface": "inside",
    "__raw__": "Oct 10 2018 12:34:56 localhost CiscoASA[999]: %ASA-6-305011: Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256",
    "appName": "CiscoASA[999]",
    "dest_interface": "outside",
    "source_ip": "172.31.98.44",
    "translation_type": "dynamic",
    "deviceId": "localhost",
    "protocol": "TCP",
    "source_port": "1772",
    "dest_ip": "100.66.98.44",
    "action": "Built",
    "dest_port": "8256",
    "timestamp": "Oct 10 2018 12:34:56"
  }
""";
    Map<String, Object> expected = fromJsonString(parsedStr, new TypeReference<>() {});
    assertEquals(expected, objects.getFirst());
  }
}
