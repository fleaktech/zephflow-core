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
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmtpSinkFlusherTest {

  private Session session;
  private SmtpSinkFlusher flusher;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty("mail.smtp.host", "localhost");
    props.setProperty("mail.smtp.port", "25");
    session = Session.getInstance(props);
    flusher = new SmtpSinkFlusher(session, "user", "pass");
  }

  @Test
  void testCreateMimeMessage() throws Exception {
    PreparedEmail email =
        new PreparedEmail(
            "from@test.com",
            List.of("to@test.com"),
            List.of("cc@test.com"),
            "Test Subject",
            "Hello World",
            "text/plain");

    MimeMessage message = flusher.createMimeMessage(email);

    assertEquals("from@test.com", ((InternetAddress) message.getFrom()[0]).getAddress());
    assertEquals(
        "to@test.com",
        ((InternetAddress) message.getRecipients(Message.RecipientType.TO)[0]).getAddress());
    assertEquals(
        "cc@test.com",
        ((InternetAddress) message.getRecipients(Message.RecipientType.CC)[0]).getAddress());
    assertEquals("Test Subject", message.getSubject());
  }

  @Test
  void testCreateMimeMessageWithMultipleRecipients() throws Exception {
    PreparedEmail email =
        new PreparedEmail(
            "from@test.com",
            List.of("to1@test.com", "to2@test.com"),
            List.of("cc1@test.com", "cc2@test.com"),
            "Multi Recipient",
            "Body",
            "text/plain");

    MimeMessage message = flusher.createMimeMessage(email);

    assertEquals(2, message.getRecipients(Message.RecipientType.TO).length);
    assertEquals(2, message.getRecipients(Message.RecipientType.CC).length);
    assertEquals(
        "to1@test.com",
        ((InternetAddress) message.getRecipients(Message.RecipientType.TO)[0]).getAddress());
    assertEquals(
        "to2@test.com",
        ((InternetAddress) message.getRecipients(Message.RecipientType.TO)[1]).getAddress());
  }

  @Test
  void testCreateMimeMessageWithNullCc() throws Exception {
    PreparedEmail email =
        new PreparedEmail(
            "from@test.com", List.of("to@test.com"), null, "No CC", "Body", "text/plain");

    MimeMessage message = flusher.createMimeMessage(email);

    assertNull(message.getRecipients(Message.RecipientType.CC));
  }

  @Test
  void testCreateMimeMessageWithEmptyCc() throws Exception {
    PreparedEmail email =
        new PreparedEmail(
            "from@test.com", List.of("to@test.com"), List.of(), "Empty CC", "Body", "text/html");

    MimeMessage message = flusher.createMimeMessage(email);

    assertNull(message.getRecipients(Message.RecipientType.CC));
  }

  @Test
  void testFlushEmptyEvents() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PreparedEmail> emptyEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result =
        flusher.flush(emptyEvents, Map.of("callingUser", "test"));

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertEquals(0, result.errorOutputList().size());
  }

  @Test
  void testClosedFlusherThrowsException() {
    flusher.close();

    SimpleSinkCommand.PreparedInputEvents<PreparedEmail> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData rawEvent = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));
    PreparedEmail email =
        new PreparedEmail(
            "from@test.com", List.of("to@test.com"), List.of(), "Subject", "Body", "text/plain");
    events.add(rawEvent, email);

    assertThrows(
        IllegalStateException.class, () -> flusher.flush(events, Map.of("callingUser", "test")));
  }

  @Test
  void testFlushWithSendFailure() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PreparedEmail> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData rawEvent = (RecordFleakData) FleakData.wrap(Map.of("key", "value"));
    PreparedEmail email =
        new PreparedEmail(
            "from@test.com", List.of("to@test.com"), List.of(), "Subject", "Body", "text/plain");
    events.add(rawEvent, email);

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of("callingUser", "test"));

    assertEquals(1, result.errorOutputList().size());
    assertEquals(0, result.successCount());
  }
}
