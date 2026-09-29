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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.S3Client;

class S3RegionalClientProviderTest {
  @Test
  void createsLazilyAndReusesCanonicalRegionAcrossRecords() {
    Function<String, S3Client> factory = mock(Function.class);
    S3Client east = mock(S3Client.class);
    S3Client west = mock(S3Client.class);
    when(factory.apply("us-east-1")).thenReturn(east);
    when(factory.apply("us-west-2")).thenReturn(west);
    S3RegionalClientProvider clients = new S3RegionalClientProvider(factory);

    verifyNoInteractions(factory);
    assertSame(east, clients.clientFor("us-east-1"));
    assertSame(west, clients.clientFor("us-west-2"));
    assertSame(east, clients.clientFor("US-EAST-1"));
    verify(factory).apply("us-east-1");
    verify(factory).apply("us-west-2");
    verifyNoMoreInteractions(factory);
    clients.close();
    verify(east).close();
    verify(west).close();
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "unknown-region", " us-east-1 "})
  void invalidRegionNeverCallsFactory(String region) {
    Function<String, S3Client> factory = mock(Function.class);
    S3RegionalClientProvider clients = new S3RegionalClientProvider(factory);
    RuntimeException error = assertThrows(RuntimeException.class, () -> clients.clientFor(region));
    assertTrue(error.getMessage().contains("failed to find region"));
    clients.close();
    verifyNoInteractions(factory);
  }

  @Test
  void closesEarlierClientWhenAnotherRegionFactoryFails() {
    S3Client east = mock(S3Client.class);
    IllegalStateException creationFailure = new IllegalStateException("factory unavailable");
    S3RegionalClientProvider clients =
        new S3RegionalClientProvider(
            region -> {
              if (region.equals("us-east-1")) {
                return east;
              }
              throw creationFailure;
            });
    clients.clientFor("us-east-1");
    assertSame(
        creationFailure,
        assertThrows(RuntimeException.class, () -> clients.clientFor("us-west-2")));
    clients.close();
    verify(east).close();
  }

  @Test
  void closeAttemptsEveryClientAndRetainsRegionAndOriginalCauses() {
    S3Client east = mock(S3Client.class);
    S3Client west = mock(S3Client.class);
    S3Client europe = mock(S3Client.class);
    RuntimeException eastError = new IllegalStateException("east close");
    RuntimeException westError = new IllegalStateException("west close");
    doThrow(eastError).when(east).close();
    doThrow(westError).when(west).close();
    S3RegionalClientProvider clients =
        new S3RegionalClientProvider(
            region ->
                switch (region) {
                  case "us-east-1" -> east;
                  case "us-west-2" -> west;
                  default -> europe;
                });
    clients.clientFor("us-east-1");
    clients.clientFor("us-west-2");
    clients.clientFor("eu-central-1");

    for (int attempt = 1; attempt <= 2; attempt++) {
      RuntimeException error = assertThrows(RuntimeException.class, clients::close);
      assertTrue(error.getMessage().contains("us-east-1"));
      assertSame(eastError, error.getCause());
      assertEquals(1, error.getSuppressed().length);
      assertTrue(error.getSuppressed()[0].getMessage().contains("us-west-2"));
      assertSame(westError, error.getSuppressed()[0].getCause());
      verify(east, times(attempt)).close();
      verify(west, times(attempt)).close();
      verify(europe, times(attempt)).close();
    }
  }

  @Test
  void nextCloseRetriesClientsAndCanSucceedAfterTransientFailure() {
    S3Client client = mock(S3Client.class);
    doThrow(new IllegalStateException("transient close")).doNothing().when(client).close();
    S3RegionalClientProvider clients = new S3RegionalClientProvider(region -> client);
    clients.clientFor("us-east-1");
    assertThrows(RuntimeException.class, clients::close);
    assertDoesNotThrow(clients::close);
    verify(client, times(2)).close();
  }

  @Test
  void cannotCreateClientAfterCleanupWhileLaterCloseStillRetriesOwnedClients() {
    Function<String, S3Client> factory = mock(Function.class);
    S3Client client = mock(S3Client.class);
    when(factory.apply("us-east-1")).thenReturn(client);
    S3RegionalClientProvider clients = new S3RegionalClientProvider(factory);
    clients.clientFor("us-east-1");
    clients.close();
    assertThrows(IllegalStateException.class, () -> clients.clientFor("us-west-2"));
    assertThrows(IllegalStateException.class, () -> clients.clientFor("us-east-1"));
    verify(factory).apply("us-east-1");
    verifyNoMoreInteractions(factory);
    clients.close();
    verify(client, times(2)).close();
  }
}
