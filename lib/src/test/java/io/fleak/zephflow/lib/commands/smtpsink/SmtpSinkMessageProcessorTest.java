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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SmtpSinkMessageProcessorTest {

  @Test
  void testBasicTemplateResolution() throws Exception {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.test.com")
            .credentialId("cred")
            .fromAddress("sender@test.com")
            .toTemplate("{{$.email}}")
            .subjectTemplate("Hello {{$.name}}")
            .bodyTemplate("Dear {{$.name}}, your order #{{$.orderId}} is ready.")
            .bodyContentType("text/plain")
            .build();

    SmtpSinkMessageProcessor processor = new SmtpSinkMessageProcessor(config);

    RecordFleakData event =
        (RecordFleakData)
            FleakData.wrap(Map.of("email", "user@example.com", "name", "John", "orderId", "12345"));

    PreparedEmail result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals("sender@test.com", result.from());
    assertEquals(List.of("user@example.com"), result.to());
    assertEquals(List.of(), result.cc());
    assertEquals("Hello John", result.subject());
    assertEquals("Dear John, your order #12345 is ready.", result.body());
    assertEquals("text/plain", result.contentType());
  }

  @Test
  void testMultipleToRecipients() throws Exception {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.test.com")
            .credentialId("cred")
            .fromAddress("sender@test.com")
            .toTemplate("{{$.email1}},{{$.email2}}")
            .subjectTemplate("Test")
            .bodyTemplate("Body")
            .build();

    SmtpSinkMessageProcessor processor = new SmtpSinkMessageProcessor(config);

    RecordFleakData event =
        (RecordFleakData) FleakData.wrap(Map.of("email1", "a@test.com", "email2", "b@test.com"));

    PreparedEmail result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals(List.of("a@test.com", "b@test.com"), result.to());
  }

  @Test
  void testCcTemplateResolution() throws Exception {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.test.com")
            .credentialId("cred")
            .fromAddress("sender@test.com")
            .toTemplate("{{$.to}}")
            .ccTemplate("{{$.cc}}")
            .subjectTemplate("Test")
            .bodyTemplate("Body")
            .build();

    SmtpSinkMessageProcessor processor = new SmtpSinkMessageProcessor(config);

    RecordFleakData event =
        (RecordFleakData) FleakData.wrap(Map.of("to", "to@test.com", "cc", "cc@test.com"));

    PreparedEmail result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals(List.of("to@test.com"), result.to());
    assertEquals(List.of("cc@test.com"), result.cc());
  }

  @Test
  void testNoCcTemplate() throws Exception {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.test.com")
            .credentialId("cred")
            .fromAddress("sender@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("Subject")
            .bodyTemplate("Body")
            .build();

    SmtpSinkMessageProcessor processor = new SmtpSinkMessageProcessor(config);

    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));

    PreparedEmail result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals(List.of(), result.cc());
  }

  @Test
  void testStaticTemplateValues() throws Exception {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.test.com")
            .credentialId("cred")
            .fromAddress("sender@test.com")
            .toTemplate("static@test.com")
            .subjectTemplate("Static Subject")
            .bodyTemplate("Static Body Content")
            .build();

    SmtpSinkMessageProcessor processor = new SmtpSinkMessageProcessor(config);

    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));

    PreparedEmail result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals(List.of("static@test.com"), result.to());
    assertEquals("Static Subject", result.subject());
    assertEquals("Static Body Content", result.body());
  }

  @Test
  void testHtmlBodyContentType() throws Exception {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.test.com")
            .credentialId("cred")
            .fromAddress("sender@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("Test")
            .bodyTemplate("<html><body>{{$.content}}</body></html>")
            .bodyContentType("text/html")
            .build();

    SmtpSinkMessageProcessor processor = new SmtpSinkMessageProcessor(config);

    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("content", "Hello World"));

    PreparedEmail result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals("<html><body>Hello World</body></html>", result.body());
    assertEquals("text/html", result.contentType());
  }
}
