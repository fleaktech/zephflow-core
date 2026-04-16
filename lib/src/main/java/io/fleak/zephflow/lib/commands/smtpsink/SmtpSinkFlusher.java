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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class SmtpSinkFlusher implements SimpleSinkCommand.Flusher<PreparedEmail> {

  private final Session session;
  private final String username;
  private final String password;
  private volatile boolean closed = false;

  public SmtpSinkFlusher(Session session, String username, String password) {
    this.session = session;
    this.username = username;
    this.password = password;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<PreparedEmail> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {

    if (closed) {
      throw new IllegalStateException("SmtpSinkFlusher is closed");
    }

    List<PreparedEmail> emails = preparedInputEvents.preparedList();
    if (emails.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    int sentCount = 0;
    long totalSize = 0;

    List<Pair<RecordFleakData, PreparedEmail>> rawAndPrepared =
        preparedInputEvents.rawAndPreparedList();

    for (int i = 0; i < emails.size(); i++) {
      PreparedEmail email = emails.get(i);
      RecordFleakData rawEvent = rawAndPrepared.get(i).getKey();
      try {
        MimeMessage message = createMimeMessage(email);
        Transport.send(message, username, password);
        sentCount++;
        totalSize += email.body() != null ? email.body().length() : 0;
      } catch (Exception e) {
        log.error("Failed to send email to {}", email.to(), e);
        errorOutputs.add(new ErrorOutput(rawEvent, e.getMessage()));
      }
    }

    log.debug("SMTP flush completed: {} sent, {} errors", sentCount, errorOutputs.size());
    return new SimpleSinkCommand.FlushResult(sentCount, totalSize, errorOutputs);
  }

  MimeMessage createMimeMessage(PreparedEmail email) throws Exception {
    MimeMessage message = new MimeMessage(session);
    message.setFrom(new InternetAddress(email.from()));

    for (String to : email.to()) {
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
    }

    if (email.cc() != null) {
      for (String cc : email.cc()) {
        message.addRecipient(Message.RecipientType.CC, new InternetAddress(cc));
      }
    }

    message.setSubject(email.subject());
    message.setContent(email.body(), email.contentType());

    return message;
  }

  @Override
  public void close() {
    closed = true;
  }
}
