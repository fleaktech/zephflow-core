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
package io.fleak.zephflow.lib.commands.activemqsource;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import jakarta.jms.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record ActiveMqSourceFetcher(
    Connection connection,
    Session session,
    MessageConsumer consumer,
    long receiveTimeoutMs,
    int maxBatchSize,
    CommitStrategy commitStrategy)
    implements Fetcher<SerializedEvent> {

  @Override
  public List<SerializedEvent> fetch() {
    List<SerializedEvent> events = new ArrayList<>();
    try {
      // Block on first receive to wait for messages
      Message message = consumer.receive(receiveTimeoutMs);
      if (message == null) {
        return events;
      }
      events.add(new SerializedEvent(null, extractBody(message), null));

      // Drain remaining available messages without blocking
      for (int i = 1; i < maxBatchSize; i++) {
        message = consumer.receiveNoWait();
        if (message == null) {
          break;
        }
        events.add(new SerializedEvent(null, extractBody(message), null));
      }
    } catch (JMSException e) {
      log.error("Error fetching from ActiveMQ", e);
    }
    log.debug("Fetched {} messages from ActiveMQ", events.size());
    return events;
  }

  @Override
  public Committer committer() {
    return () -> {
      try {
        session.commit();
      } catch (JMSException e) {
        throw new IOException("Failed to commit JMS session", e);
      }
    };
  }

  @Override
  public CommitStrategy commitStrategy() {
    return commitStrategy;
  }

  @Override
  public void close() throws IOException {
    closeQuietly(consumer);
    closeQuietly(session);
    closeQuietly(connection);
  }

  private byte[] extractBody(Message message) throws JMSException {
    if (message instanceof BytesMessage bm) {
      byte[] body = new byte[(int) bm.getBodyLength()];
      bm.readBytes(body);
      return body;
    } else if (message instanceof TextMessage tm) {
      return tm.getText().getBytes(StandardCharsets.UTF_8);
    }
    throw new JMSException("Unsupported message type: " + message.getClass().getName());
  }

  static void closeQuietly(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        log.warn("Failed to close JMS resource", e);
      }
    }
  }
}
