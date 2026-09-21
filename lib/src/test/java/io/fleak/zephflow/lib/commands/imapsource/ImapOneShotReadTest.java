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

import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImapOneShotReadTest {
  private Store store;
  private Folder folder;

  @BeforeEach
  void setup() throws Exception {
    store = mock(Store.class);
    folder = mock(Folder.class);
    when(store.getFolder("INBOX")).thenReturn(folder);
  }

  private ImapSourceFetcher fetcher() {
    return new ImapSourceFetcher(store, "INBOX", null, false, false, 10);
  }

  private static MimeMessage message(String text) throws Exception {
    var message = new MimeMessage(Session.getInstance(new Properties()));
    message.setText(text);
    message.saveChanges();
    return message;
  }

  @Test
  void firstReadSlowerThanQueueTimeoutStillCompletesWithDataAndReadOnly() throws Exception {
    var mail = message("hello");
    when(folder.getMessages())
        .thenAnswer(
            invocation -> {
              Thread.sleep(150);
              return new Message[] {mail};
            });
    var output = new ArrayList<EmailMessage>();
    try (var fetcher = fetcher()) {
      var result = fetcher.fetchOnce(output::add, () -> false, Long.MAX_VALUE);
      assertEquals(ImapSourceFetcher.OneShotStatus.COMPLETED, result.status());
      assertEquals(1, result.delivered());
      assertNull(result.error());
      assertEquals("hello", output.getFirst().bodyText());
      assertFalse(mail.isSet(jakarta.mail.Flags.Flag.SEEN));
    }
    verify(folder).open(Folder.READ_ONLY);
    verify(folder).close(false);
    verify(folder, times(1)).getMessages();
  }

  @Test
  void emptyAndInvalidFolderHaveDifferentOutcomes() throws Exception {
    when(folder.getMessages()).thenReturn(new Message[0]);
    try (var fetcher = fetcher()) {
      var result = fetcher.fetchOnce(ignored -> fail(), () -> false, Long.MAX_VALUE);
      assertEquals(ImapSourceFetcher.OneShotStatus.COMPLETED, result.status());
      assertEquals(0, result.delivered());
      assertNull(result.error());
    }
    var failure = new MessagingException("no such folder");
    doThrow(failure).when(folder).open(Folder.READ_ONLY);
    try (var fetcher = fetcher()) {
      var result = fetcher.fetchOnce(ignored -> fail(), () -> false, Long.MAX_VALUE);
      assertEquals(ImapSourceFetcher.OneShotStatus.FAILED, result.status());
      assertSame(failure, result.error());
    }
  }

  @Test
  void conversionFailureAfterGoodMessageIsExplicitPartialFailure() throws Exception {
    var bad = spy(message("bad"));
    var failure = new IOException("message failed");
    doThrow(failure).when(bad).getContent();
    when(folder.getMessages()).thenReturn(new Message[] {message("good"), bad});
    var output = new ArrayList<EmailMessage>();
    try (var fetcher = fetcher()) {
      var result = fetcher.fetchOnce(output::add, () -> false, Long.MAX_VALUE);
      assertEquals(ImapSourceFetcher.OneShotStatus.FAILED, result.status());
      assertEquals(1, result.delivered());
      assertSame(failure, result.error());
      assertEquals(1, output.size());
    }
    verify(folder).close(false);
  }

  @Test
  void cancellationDoesNotReadNextMessageAndStillCloses() throws Exception {
    var unread = spy(message("next"));
    when(folder.getMessages()).thenReturn(new Message[] {message("first"), unread});
    var cancelled = new AtomicBoolean();
    try (var fetcher = fetcher()) {
      var result =
          fetcher.fetchOnce(ignored -> cancelled.set(true), cancelled::get, Long.MAX_VALUE);
      assertEquals(ImapSourceFetcher.OneShotStatus.CANCELLED, result.status());
      assertEquals(1, result.delivered());
    }
    verify(unread, never()).getContent();
    verify(folder).close(false);
  }

  @Test
  void elapsedDeadlineAfterBlockingEmptyReadIsNotCompletedEmpty() throws Exception {
    when(folder.getMessages())
        .thenAnswer(
            invocation -> {
              Thread.sleep(30);
              return new Message[0];
            });
    try (var fetcher = fetcher()) {
      var result =
          fetcher.fetchOnce(
              ignored -> fail(),
              () -> false,
              System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(10));
      assertEquals(ImapSourceFetcher.OneShotStatus.TIMED_OUT, result.status());
    }
    verify(folder).close(false);
  }

  @Test
  void cancelledBeforeStartDoesNotContactStore() throws Exception {
    try (var fetcher = fetcher()) {
      var result = fetcher.fetchOnce(ignored -> fail(), () -> true, Long.MAX_VALUE);
      assertEquals(ImapSourceFetcher.OneShotStatus.CANCELLED, result.status());
      verify(store, never()).getFolder(anyString());
    }
  }

  @Test
  void consumerFailurePropagatesWithoutLosingClose() throws Exception {
    when(folder.getMessages()).thenReturn(new Message[] {message("first")});
    RuntimeException failure = new RuntimeException("storage failed");
    try (var fetcher = fetcher()) {
      assertSame(
          failure,
          assertThrows(
              RuntimeException.class,
              () ->
                  fetcher.fetchOnce(
                      ignored -> {
                        throw failure;
                      },
                      () -> false,
                      Long.MAX_VALUE)));
    }
    verify(folder).close(false);
  }
}
