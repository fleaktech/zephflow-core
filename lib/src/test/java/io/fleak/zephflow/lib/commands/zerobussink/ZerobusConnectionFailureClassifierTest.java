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
package io.fleak.zephflow.lib.commands.zerobussink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.sink.RetriableConnectionException;
import io.fleak.zephflow.lib.commands.sink.UnknownSinkCommitStateException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class ZerobusConnectionFailureClassifierTest {

  private final ZerobusConnectionFailureClassifier classifier =
      new ZerobusConnectionFailureClassifier();

  /** Stand-in for gRPC's StatusRuntimeException, matched structurally by class name + message. */
  static class StatusRuntimeException extends RuntimeException {
    StatusRuntimeException(String message) {
      super(message);
    }
  }

  @Test
  void ioExceptionInCauseChainIsConnectionFailure() {
    Throwable wrapped =
        new UnknownSinkCommitStateException("commit state unknown", new IOException("broken pipe"));
    assertTrue(classifier.isConnectionFailure(wrapped));
  }

  @Test
  void retriableConnectionExceptionIsConnectionFailure() {
    assertTrue(
        classifier.isConnectionFailure(
            new RetriableConnectionException("reconnect failed", new RuntimeException("nope"))));
  }

  @Test
  void retryableGrpcStatusIsConnectionFailure() {
    assertTrue(
        classifier.isConnectionFailure(new StatusRuntimeException("UNAVAILABLE: io timeout")));
    assertTrue(
        classifier.isConnectionFailure(
            new UnknownSinkCommitStateException(
                "unknown", new StatusRuntimeException("DEADLINE_EXCEEDED: deadline"))));
  }

  @Test
  void nonRetryableGrpcStatusIsNotConnectionFailure() {
    assertFalse(
        classifier.isConnectionFailure(new StatusRuntimeException("INVALID_ARGUMENT: bad schema")));
  }

  @Test
  void plainPermanentFailureIsNotConnectionFailure() {
    assertFalse(classifier.isConnectionFailure(new IllegalArgumentException("table not found")));
    assertFalse(
        classifier.isConnectionFailure(
            new UnknownSinkCommitStateException("unknown", new IllegalStateException("schema"))));
  }
}
