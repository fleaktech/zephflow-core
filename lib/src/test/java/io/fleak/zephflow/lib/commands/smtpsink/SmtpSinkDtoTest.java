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
package io.fleak.zephflow.lib.commands.smtpsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SmtpSinkDtoTest {

  @Test
  void testConfigParsing() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "smtp.example.com");
    configMap.put("port", 465);
    configMap.put("credentialId", "smtp-cred");
    configMap.put("fromAddress", "noreply@example.com");
    configMap.put("toTemplate", "{{$.email}}");
    configMap.put("ccTemplate", "{{$.ccEmail}}");
    configMap.put("subjectTemplate", "Order {{$.orderId}}");
    configMap.put("bodyTemplate", "Your order {{$.orderId}} is ready.");
    configMap.put("bodyContentType", "text/html");
    configMap.put("useTls", false);

    SmtpSinkDto.Config config = OBJECT_MAPPER.convertValue(configMap, SmtpSinkDto.Config.class);

    assertEquals("smtp.example.com", config.getHost());
    assertEquals(465, config.getPort());
    assertEquals("smtp-cred", config.getCredentialId());
    assertEquals("noreply@example.com", config.getFromAddress());
    assertEquals("{{$.email}}", config.getToTemplate());
    assertEquals("{{$.ccEmail}}", config.getCcTemplate());
    assertEquals("Order {{$.orderId}}", config.getSubjectTemplate());
    assertEquals("Your order {{$.orderId}} is ready.", config.getBodyTemplate());
    assertEquals("text/html", config.getBodyContentType());
    assertFalse(config.getUseTls());
  }

  @Test
  void testConfigParsingWithDefaults() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("host", "smtp.example.com");
    configMap.put("credentialId", "cred");
    configMap.put("fromAddress", "from@test.com");
    configMap.put("toTemplate", "to@test.com");
    configMap.put("subjectTemplate", "subj");
    configMap.put("bodyTemplate", "body");

    SmtpSinkDto.Config config = OBJECT_MAPPER.convertValue(configMap, SmtpSinkDto.Config.class);

    assertEquals(SmtpSinkDto.DEFAULT_PORT, config.getPort());
    assertEquals(SmtpSinkDto.DEFAULT_BODY_CONTENT_TYPE, config.getBodyContentType());
    assertEquals(SmtpSinkDto.DEFAULT_USE_TLS, config.getUseTls());
    assertNull(config.getCcTemplate());
  }

  @Test
  void testConfigBuilder() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.gmail.com")
            .port(587)
            .credentialId("gmail-cred")
            .fromAddress("sender@gmail.com")
            .toTemplate("{{$.recipient}}")
            .subjectTemplate("Notification")
            .bodyTemplate("Hello {{$.name}}")
            .bodyContentType("text/html")
            .useTls(true)
            .build();

    assertEquals("smtp.gmail.com", config.getHost());
    assertEquals(587, config.getPort());
    assertEquals("gmail-cred", config.getCredentialId());
    assertEquals("sender@gmail.com", config.getFromAddress());
    assertEquals("text/html", config.getBodyContentType());
    assertTrue(config.getUseTls());
  }
}
