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

import io.fleak.zephflow.lib.commands.source.Fetcher;
import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.search.*;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImapSourceFetcher implements Fetcher<EmailMessage> {

  private final Store store;
  private final String folderName;
  private final String searchCriteria;
  private final boolean markAsRead;
  private final boolean includeAttachments;
  private final int maxMessages;

  private Folder folder;

  public ImapSourceFetcher(
      Store store,
      String folderName,
      String searchCriteria,
      boolean markAsRead,
      boolean includeAttachments,
      int maxMessages) {
    this.store = store;
    this.folderName = folderName;
    this.searchCriteria = searchCriteria;
    this.markAsRead = markAsRead;
    this.includeAttachments = includeAttachments;
    this.maxMessages = maxMessages;
  }

  @Override
  public List<EmailMessage> fetch() {
    List<EmailMessage> emails = new ArrayList<>();
    try {
      if (folder == null || !folder.isOpen()) {
        folder = store.getFolder(folderName);
        folder.open(Folder.READ_WRITE);
      }

      SearchTerm searchTerm = parseSearchCriteria(searchCriteria);
      Message[] messages = searchTerm != null ? folder.search(searchTerm) : folder.getMessages();

      int limit = Math.min(messages.length, maxMessages);
      for (int i = 0; i < limit; i++) {
        try {
          emails.add(convertMessage(messages[i]));
          if (markAsRead) {
            messages[i].setFlag(Flags.Flag.SEEN, true);
          }
        } catch (Exception e) {
          log.error("Error processing email message", e);
        }
      }

      folder.close(false);
      folder = null;
    } catch (Exception e) {
      log.error("Error fetching emails from IMAP", e);
    }
    log.debug("Fetched {} emails from IMAP folder {}", emails.size(), folderName);
    return emails;
  }

  EmailMessage convertMessage(Message message) throws MessagingException, IOException {
    String messageId = getMessageId(message);
    String from = extractFrom(message);
    List<String> to = extractAddresses(message, Message.RecipientType.TO);
    List<String> cc = extractAddresses(message, Message.RecipientType.CC);
    String subject = message.getSubject();
    Map<String, String> headers = extractHeaders(message);
    Instant receivedDate =
        message.getReceivedDate() != null ? message.getReceivedDate().toInstant() : null;

    String bodyText = null;
    String bodyHtml = null;
    List<EmailMessage.Attachment> attachments = new ArrayList<>();

    Object content = message.getContent();
    if (content instanceof String textContent) {
      if (message.isMimeType("text/html")) {
        bodyHtml = textContent;
      } else {
        bodyText = textContent;
      }
    } else if (content instanceof MimeMultipart multipart) {
      BodyContent bodyContent = extractMultipartContent(multipart, attachments);
      bodyText = bodyContent.text;
      bodyHtml = bodyContent.html;
    }

    return new EmailMessage(
        messageId, from, to, cc, subject, bodyText, bodyHtml, headers, attachments, receivedDate);
  }

  private String getMessageId(Message message) throws MessagingException {
    String[] messageIdHeader = message.getHeader("Message-ID");
    if (messageIdHeader != null && messageIdHeader.length > 0) {
      return messageIdHeader[0];
    }
    return null;
  }

  private String extractFrom(Message message) throws MessagingException {
    Address[] fromAddresses = message.getFrom();
    if (fromAddresses != null && fromAddresses.length > 0) {
      if (fromAddresses[0] instanceof InternetAddress ia) {
        return ia.getAddress();
      }
      return fromAddresses[0].toString();
    }
    return null;
  }

  private List<String> extractAddresses(Message message, Message.RecipientType type)
      throws MessagingException {
    Address[] addresses = message.getRecipients(type);
    if (addresses == null) {
      return List.of();
    }
    List<String> result = new ArrayList<>();
    for (Address address : addresses) {
      if (address instanceof InternetAddress ia) {
        result.add(ia.getAddress());
      } else {
        result.add(address.toString());
      }
    }
    return result;
  }

  private Map<String, String> extractHeaders(Message message) throws MessagingException {
    Map<String, String> headers = new LinkedHashMap<>();
    Enumeration<Header> allHeaders = message.getAllHeaders();
    while (allHeaders.hasMoreElements()) {
      Header header = allHeaders.nextElement();
      headers.put(header.getName(), header.getValue());
    }
    return headers;
  }

  private BodyContent extractMultipartContent(
      MimeMultipart multipart, List<EmailMessage.Attachment> attachments)
      throws MessagingException, IOException {
    String text = null;
    String html = null;

    for (int i = 0; i < multipart.getCount(); i++) {
      BodyPart part = multipart.getBodyPart(i);
      String disposition = part.getDisposition();

      if (disposition != null
          && (disposition.equalsIgnoreCase(Part.ATTACHMENT)
              || disposition.equalsIgnoreCase(Part.INLINE))) {
        if (includeAttachments) {
          attachments.add(extractAttachment(part));
        }
      } else if (part.isMimeType("text/plain") && text == null) {
        text = (String) part.getContent();
      } else if (part.isMimeType("text/html") && html == null) {
        html = (String) part.getContent();
      } else if (part.getContent() instanceof MimeMultipart nestedMultipart) {
        BodyContent nested = extractMultipartContent(nestedMultipart, attachments);
        if (text == null) {
          text = nested.text;
        }
        if (html == null) {
          html = nested.html;
        }
      }
    }

    return new BodyContent(text, html);
  }

  private EmailMessage.Attachment extractAttachment(BodyPart part)
      throws MessagingException, IOException {
    String filename = part.getFileName();
    String contentType = part.getContentType();
    try (InputStream is = part.getInputStream()) {
      byte[] bytes = is.readAllBytes();
      String contentBase64 = Base64.getEncoder().encodeToString(bytes);
      return new EmailMessage.Attachment(filename, contentType, contentBase64);
    }
  }

  static SearchTerm parseSearchCriteria(String criteria) {
    if (criteria == null || criteria.isBlank()) {
      return null;
    }

    String trimmed = criteria.trim();

    if (trimmed.equalsIgnoreCase("UNSEEN")) {
      return new FlagTerm(new Flags(Flags.Flag.SEEN), false);
    }

    if (trimmed.equalsIgnoreCase("SEEN")) {
      return new FlagTerm(new Flags(Flags.Flag.SEEN), true);
    }

    if (trimmed.toUpperCase().startsWith("SINCE ")) {
      String dateStr = trimmed.substring(6).trim();
      try {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
        return new ReceivedDateTerm(ComparisonTerm.GE, date);
      } catch (ParseException e) {
        throw new IllegalArgumentException("Invalid date format in SINCE criteria: " + dateStr, e);
      }
    }

    if (trimmed.toUpperCase().startsWith("FROM ")) {
      String addr = trimmed.substring(5).trim();
      return new FromStringTerm(addr);
    }

    if (trimmed.toUpperCase().startsWith("SUBJECT ")) {
      String text = trimmed.substring(8).trim();
      return new SubjectTerm(text);
    }

    throw new IllegalArgumentException("Unsupported search criteria: " + criteria);
  }

  @Override
  public void close() throws IOException {
    try {
      if (folder != null && folder.isOpen()) {
        folder.close(false);
      }
    } catch (MessagingException e) {
      log.warn("Failed to close IMAP folder", e);
    }
    try {
      if (store != null && store.isConnected()) {
        store.close();
      }
    } catch (MessagingException e) {
      log.warn("Failed to close IMAP store", e);
    }
  }

  private record BodyContent(String text, String html) {}
}
