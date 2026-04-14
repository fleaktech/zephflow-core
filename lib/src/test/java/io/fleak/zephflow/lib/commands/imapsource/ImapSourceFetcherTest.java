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

import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.search.FlagTerm;
import jakarta.mail.search.FromStringTerm;
import jakarta.mail.search.ReceivedDateTerm;
import jakarta.mail.search.SubjectTerm;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImapSourceFetcherTest {

  private Store mockStore;
  private Folder mockFolder;

  @BeforeEach
  void setUp() throws Exception {
    mockStore = mock(Store.class);
    mockFolder = mock(Folder.class);
    when(mockStore.getFolder("INBOX")).thenReturn(mockFolder);
  }

  @Test
  void testFetchWithNoMessages() throws Exception {
    when(mockFolder.isOpen()).thenReturn(false);
    when(mockFolder.search(any())).thenReturn(new Message[0]);

    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", true, false, 100);

    List<EmailMessage> result = fetcher.fetch();

    assertNotNull(result);
    assertTrue(result.isEmpty());
    verify(mockFolder).open(Folder.READ_WRITE);
  }

  @Test
  void testFetchWithSimpleTextMessage() throws Exception {
    when(mockFolder.isOpen()).thenReturn(false);

    Message mockMessage =
        createMockTextMessage(
            "<msg1@test.com>",
            "sender@test.com",
            new String[] {"to@test.com"},
            null,
            "Test Subject",
            "Hello World",
            "text/plain",
            new Date(1700000000000L));

    when(mockFolder.search(any())).thenReturn(new Message[] {mockMessage});

    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", true, false, 100);

    List<EmailMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
    EmailMessage email = result.getFirst();
    assertEquals("<msg1@test.com>", email.messageId());
    assertEquals("sender@test.com", email.from());
    assertEquals(List.of("to@test.com"), email.to());
    assertEquals("Test Subject", email.subject());
    assertEquals("Hello World", email.bodyText());
    assertNull(email.bodyHtml());

    verify(mockMessage).setFlag(Flags.Flag.SEEN, true);
  }

  @Test
  void testFetchWithHtmlMessage() throws Exception {
    when(mockFolder.isOpen()).thenReturn(false);

    Message mockMessage =
        createMockTextMessage(
            "<msg2@test.com>",
            "sender@test.com",
            new String[] {"to@test.com"},
            null,
            "HTML Subject",
            "<html>Hello</html>",
            "text/html",
            new Date());

    when(mockFolder.search(any())).thenReturn(new Message[] {mockMessage});

    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", false, false, 100);

    List<EmailMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
    assertNull(result.getFirst().bodyText());
    assertEquals("<html>Hello</html>", result.getFirst().bodyHtml());
    verify(mockMessage, never()).setFlag(any(), anyBoolean());
  }

  @Test
  void testFetchWithMaxMessagesLimit() throws Exception {
    when(mockFolder.isOpen()).thenReturn(false);

    Message[] messages = new Message[10];
    for (int i = 0; i < 10; i++) {
      messages[i] =
          createMockTextMessage(
              "<msg" + i + "@test.com>",
              "sender@test.com",
              new String[] {"to@test.com"},
              null,
              "Subject " + i,
              "Body " + i,
              "text/plain",
              new Date());
    }

    when(mockFolder.search(any())).thenReturn(messages);

    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", false, false, 3);

    List<EmailMessage> result = fetcher.fetch();

    assertEquals(3, result.size());
  }

  @Test
  void testFetchWithNullSearchCriteria() throws Exception {
    when(mockFolder.isOpen()).thenReturn(false);
    when(mockFolder.getMessages()).thenReturn(new Message[0]);

    ImapSourceFetcher fetcher = new ImapSourceFetcher(mockStore, "INBOX", null, false, false, 100);

    List<EmailMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(mockFolder).getMessages();
    verify(mockFolder, never()).search(any());
  }

  @Test
  void testFetchWithCcRecipients() throws Exception {
    when(mockFolder.isOpen()).thenReturn(false);

    Message mockMessage =
        createMockTextMessage(
            "<msg-cc@test.com>",
            "sender@test.com",
            new String[] {"to@test.com"},
            new String[] {"cc1@test.com", "cc2@test.com"},
            "CC Subject",
            "Body",
            "text/plain",
            new Date());

    when(mockFolder.search(any())).thenReturn(new Message[] {mockMessage});

    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", false, false, 100);

    List<EmailMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
    assertEquals(List.of("cc1@test.com", "cc2@test.com"), result.getFirst().cc());
  }

  @Test
  void testParseSearchCriteriaUnseen() {
    var term = ImapSourceFetcher.parseSearchCriteria("UNSEEN");
    assertInstanceOf(FlagTerm.class, term);
  }

  @Test
  void testParseSearchCriteriaSeen() {
    var term = ImapSourceFetcher.parseSearchCriteria("SEEN");
    assertInstanceOf(FlagTerm.class, term);
  }

  @Test
  void testParseSearchCriteriaSince() {
    var term = ImapSourceFetcher.parseSearchCriteria("SINCE 2025-01-01");
    assertInstanceOf(ReceivedDateTerm.class, term);
  }

  @Test
  void testParseSearchCriteriaFrom() {
    var term = ImapSourceFetcher.parseSearchCriteria("FROM user@example.com");
    assertInstanceOf(FromStringTerm.class, term);
  }

  @Test
  void testParseSearchCriteriaSubject() {
    var term = ImapSourceFetcher.parseSearchCriteria("SUBJECT Important");
    assertInstanceOf(SubjectTerm.class, term);
  }

  @Test
  void testParseSearchCriteriaNull() {
    assertNull(ImapSourceFetcher.parseSearchCriteria(null));
    assertNull(ImapSourceFetcher.parseSearchCriteria(""));
    assertNull(ImapSourceFetcher.parseSearchCriteria("   "));
  }

  @Test
  void testParseSearchCriteriaUnsupported() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ImapSourceFetcher.parseSearchCriteria("INVALID_CRITERIA"));
  }

  @Test
  void testParseSearchCriteriaInvalidDate() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ImapSourceFetcher.parseSearchCriteria("SINCE not-a-date"));
  }

  @Test
  void testClose() throws Exception {
    when(mockStore.isConnected()).thenReturn(true);

    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", true, false, 100);

    fetcher.close();

    verify(mockStore).close();
  }

  @Test
  void testIsExhaustedReturnsFalse() {
    ImapSourceFetcher fetcher =
        new ImapSourceFetcher(mockStore, "INBOX", "UNSEEN", true, false, 100);

    assertFalse(fetcher.isExhausted());
  }

  private Message createMockTextMessage(
      String messageId,
      String from,
      String[] to,
      String[] cc,
      String subject,
      String body,
      String mimeType,
      Date receivedDate)
      throws Exception {
    Message message = mock(Message.class);

    when(message.getHeader("Message-ID")).thenReturn(new String[] {messageId});
    when(message.getFrom()).thenReturn(new Address[] {new InternetAddress(from)});

    Address[] toAddresses = new Address[to.length];
    for (int i = 0; i < to.length; i++) {
      toAddresses[i] = new InternetAddress(to[i]);
    }
    when(message.getRecipients(Message.RecipientType.TO)).thenReturn(toAddresses);

    if (cc != null) {
      Address[] ccAddresses = new Address[cc.length];
      for (int i = 0; i < cc.length; i++) {
        ccAddresses[i] = new InternetAddress(cc[i]);
      }
      when(message.getRecipients(Message.RecipientType.CC)).thenReturn(ccAddresses);
    }

    when(message.getSubject()).thenReturn(subject);
    when(message.getContent()).thenReturn(body);
    when(message.isMimeType("text/html")).thenReturn("text/html".equals(mimeType));
    when(message.isMimeType("text/plain")).thenReturn("text/plain".equals(mimeType));
    when(message.getReceivedDate()).thenReturn(receivedDate);

    Vector<Header> headers = new Vector<>();
    headers.add(new Header("Subject", subject));
    headers.add(new Header("From", from));
    Enumeration<Header> headerEnum = headers.elements();
    when(message.getAllHeaders()).thenReturn(headerEnum);

    return message;
  }
}
