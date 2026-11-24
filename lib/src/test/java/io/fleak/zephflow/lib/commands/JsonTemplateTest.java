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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.structure.FleakData;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonTemplateTest {

  private static final ConfigurationPath PATH = ConfigurationPath.of("payloadTemplate");
  private static final String RULE = "ET \"MALWARE\" C:\\x\nnext";

  private static final FleakData EVENT =
      FleakData.wrap(
          Map.of(
              "rule",
              RULE,
              "count",
              3,
              "ok",
              true,
              "device",
              Map.of("ip", "10.0.0.1"),
              "tags",
              List.of("a", "b"),
              "payload",
              Map.of("text", RULE, "n", 1)));

  private static JsonNode render(String template) throws Exception {
    return OBJECT_MAPPER.readTree(JsonTemplate.create(template, PATH).render(EVENT));
  }

  @Test
  void mixedStringWithSpecialCharactersRendersValidJson() throws Exception {
    JsonNode body = render("{\"text\": \"alert: {{$.rule}} n={{ $.count }}\"}");
    assertEquals("alert: " + RULE + " n=3", body.get("text").asText());
  }

  @Test
  void singlePlaceholderStringKeepsTheValueType() throws Exception {
    JsonNode body =
        render(
            "{\"n\": \"{{$.count}}\", \"ok\": \"{{$.ok}}\", \"dev\": \"{{ $.device }}\","
                + " \"tags\": \"{{$.tags}}\", \"rule\": \"{{$.rule}}\", \"geo\": \"{{$.geo}}\"}");
    assertEquals(3, body.get("n").asInt());
    assertTrue(body.get("n").isNumber());
    assertTrue(body.get("ok").booleanValue());
    assertEquals("10.0.0.1", body.get("dev").get("ip").asText());
    assertEquals(
        List.of("a", "b"),
        List.of(body.get("tags").get(0).asText(), body.get("tags").get(1).asText()));
    assertEquals(RULE, body.get("rule").asText());
    assertTrue(body.get("geo").isNull());
  }

  @Test
  void wholeTemplatePlaceholderSerializesARecordAsTheBody() throws Exception {
    JsonNode body = render("\"{{$.payload}}\"");
    assertEquals(RULE, body.get("text").asText());
    assertEquals(1, body.get("n").asInt());
  }

  @Test
  void missingValueInsideMixedStringRendersAsEmpty() throws Exception {
    assertEquals("geo=", render("{\"text\": \"geo={{$.geo.country}}\"}").get("text").asText());
  }

  @Test
  void nestedObjectsAndArraysRenderAtEveryDepth() throws Exception {
    JsonNode body =
        render(
            "{\"blocks\": [{\"type\": \"section\", \"text\": {\"t\": \"{{$.rule}}\"}},"
                + " [\"{{$.count}}\", 7, null, false]]}");
    assertEquals(RULE, body.get("blocks").get(0).get("text").get("t").asText());
    assertEquals("section", body.get("blocks").get(0).get("type").asText());
    assertEquals(3, body.get("blocks").get(1).get(0).asInt());
    assertEquals(7, body.get("blocks").get(1).get(1).asInt());
    assertTrue(body.get("blocks").get(1).get(2).isNull());
  }

  @Test
  void templateWithoutPlaceholdersRendersItself() throws Exception {
    assertEquals(OBJECT_MAPPER.readTree("{\"a\": [1, \"x\"]}"), render("{\"a\": [1, \"x\"]}"));
  }

  @Test
  void placeholderOutsideQuotesRendersAJsonValue() throws Exception {
    JsonNode body =
        render(
            "{\"payload\": {{$.payload}}, \"n\": {{ $.count }}, \"ok\": {{$.ok}},"
                + " \"rule\": {{$.rule}}, \"geo\": {{$.geo}}, \"list\": [{{$.tags}}, 1]}");
    assertEquals(RULE, body.get("payload").get("text").asText());
    assertTrue(body.get("n").isNumber());
    assertEquals(3, body.get("n").asInt());
    assertTrue(body.get("ok").booleanValue());
    assertEquals(RULE, body.get("rule").asText());
    assertTrue(body.get("geo").isNull());
    assertEquals("a", body.get("list").get(0).get(0).asText());
  }

  @Test
  void wholeTemplateUnquotedPlaceholderSerializesTheRecord() throws Exception {
    assertEquals(RULE, render("{{$.payload}}").get("text").asText());
  }

  @Test
  void invalidJsonAroundAPlaceholderIsAConfigurationErrorThatDoesNotEchoTheTemplate() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> JsonTemplate.create("{\"n\": 1{{$.secret-token}}}", PATH));
    assertEquals(PATH, failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("placeholder"));
    assertFalse(failure.issues().getFirst().message().contains("secret-token"));
  }

  @Test
  void plainTextTemplateFailsWithTheGuidanceAsTheExceptionMessage() {
    // The test run panel shows getMessage(), so it must carry the guidance, not the parser's
    // report, which quotes the template.
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> JsonTemplate.create("Alert secret-token {{$.rule}}", PATH));
    assertEquals(failure.issues().getFirst().message(), failure.getMessage());
    assertFalse(failure.getMessage().contains("secret-token"));
    assertFalse(failure.getMessage().contains("Alert"));
  }

  @Test
  void unquotedPlaceholderAsAnObjectKeyIsAConfigurationError() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> JsonTemplate.create("{ {{$.k}}: 1}", PATH));
    assertEquals(PATH, failure.issues().getFirst().path());
  }

  @Test
  void placeholderWithQuotesInsideAStringValueStaysInTheString() throws Exception {
    assertEquals(
        "say \"" + RULE + "\"", render("{\"t\": \"say \\\"{{$.rule}}\\\"\"}").get("t").asText());
  }

  @Test
  void placeholderInAnObjectKeyIsAConfigurationError() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> JsonTemplate.create("{\"{{$.k}}\": 1}", PATH));
    assertEquals(PATH, failure.issues().getFirst().path());
  }
}
