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
package io.fleak.zephflow.lib.commands.imapsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EmailRawDataConverterTest {

  private EmailRawDataConverter converter;
  private SourceExecutionContext<?> mockContext;

  @BeforeEach
  void setUp() {
    converter = new EmailRawDataConverter();
    mockContext = mock(SourceExecutionContext.class);
    when(mockContext.dataSizeCounter()).thenReturn(mock());
    when(mockContext.inputEventCounter()).thenReturn(mock());
    when(mockContext.deserializeFailureCounter()).thenReturn(mock());
  }

  @Test
  void testConvertBasicEmail() {
    Instant now = Instant.parse("2025-01-15T10:30:00Z");
    EmailMessage email =
        new EmailMessage(
            "<msg123@example.com>",
            "sender@example.com",
            List.of("recipient@example.com"),
            List.of("cc@example.com"),
            "Test Subject",
            "Hello World",
            "<html>Hello World</html>",
            Map.of("Subject", "Test Subject"),
            List.of(),
            now);

    ConvertedResult<EmailMessage> result = converter.convert(email, mockContext);

    assertNotNull(result.transformedData());
    assertNull(result.error());
    assertEquals(1, result.transformedData().size());

    RecordFleakData record = result.transformedData().getFirst();
    Map<String, Object> unwrapped = record.unwrap();

    assertEquals("<msg123@example.com>", unwrapped.get("messageId"));
    assertEquals("sender@example.com", unwrapped.get("from"));
    assertEquals(List.of("recipient@example.com"), unwrapped.get("to"));
    assertEquals(List.of("cc@example.com"), unwrapped.get("cc"));
    assertEquals("Test Subject", unwrapped.get("subject"));
    assertEquals("Hello World", unwrapped.get("bodyText"));
    assertEquals("<html>Hello World</html>", unwrapped.get("bodyHtml"));
    assertEquals("2025-01-15T10:30:00Z", unwrapped.get("receivedDate"));
  }

  @Test
  void testConvertEmailWithAttachments() {
    EmailMessage.Attachment attachment =
        new EmailMessage.Attachment("file.pdf", "application/pdf", "dGVzdA==");
    EmailMessage email =
        new EmailMessage(
            "<msg456@example.com>",
            "sender@example.com",
            List.of("to@example.com"),
            List.of(),
            "With Attachment",
            "See attached",
            null,
            Map.of(),
            List.of(attachment),
            null);

    ConvertedResult<EmailMessage> result = converter.convert(email, mockContext);

    assertNotNull(result.transformedData());
    RecordFleakData record = result.transformedData().getFirst();
    Map<String, Object> unwrapped = record.unwrap();

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> attachments =
        (List<Map<String, Object>>) unwrapped.get("attachments");
    assertEquals(1, attachments.size());
    assertEquals("file.pdf", attachments.getFirst().get("filename"));
    assertEquals("application/pdf", attachments.getFirst().get("contentType"));
    assertEquals("dGVzdA==", attachments.getFirst().get("contentBase64"));
  }

  @Test
  void testConvertEmailWithNullFields() {
    EmailMessage email =
        new EmailMessage(
            null, null, List.of(), List.of(), null, null, null, Map.of(), List.of(), null);

    ConvertedResult<EmailMessage> result = converter.convert(email, mockContext);

    assertNotNull(result.transformedData());
    assertEquals(1, result.transformedData().size());

    RecordFleakData record = result.transformedData().getFirst();
    Map<String, Object> unwrapped = record.unwrap();
    assertNull(unwrapped.get("messageId"));
    assertNull(unwrapped.get("from"));
    assertNull(unwrapped.get("receivedDate"));
  }

  @Test
  void testToMap() {
    Instant now = Instant.parse("2025-06-01T12:00:00Z");
    EmailMessage email =
        new EmailMessage(
            "<id@test.com>",
            "from@test.com",
            List.of("to1@test.com", "to2@test.com"),
            List.of(),
            "Subject",
            "text body",
            "<p>html body</p>",
            Map.of("X-Custom", "value"),
            List.of(),
            now);

    Map<String, Object> map = EmailRawDataConverter.toMap(email);

    assertEquals("<id@test.com>", map.get("messageId"));
    assertEquals("from@test.com", map.get("from"));
    assertEquals(List.of("to1@test.com", "to2@test.com"), map.get("to"));
    assertEquals(List.of(), map.get("cc"));
    assertEquals("Subject", map.get("subject"));
    assertEquals("text body", map.get("bodyText"));
    assertEquals("<p>html body</p>", map.get("bodyHtml"));
    assertEquals("2025-06-01T12:00:00Z", map.get("receivedDate"));
    assertEquals(List.of(), map.get("attachments"));
  }
}
