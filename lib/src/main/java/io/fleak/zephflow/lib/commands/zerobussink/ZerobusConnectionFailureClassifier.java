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

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import io.fleak.zephflow.lib.commands.sink.ConnectionFailureClassifier;
import io.fleak.zephflow.lib.commands.sink.RetriableConnectionException;
import java.io.IOException;
import java.util.Set;

/**
 * Classifies a Zerobus flush failure as a transient connectivity failure (buffer + retry) versus a
 * permanent one (drop / normal error path).
 *
 * <p>The Zerobus flusher collapses mid-stream send failures into {@code
 * UnknownSinkCommitStateException} and reconnect failures into {@link
 * RetriableConnectionException}, so we walk the whole cause chain and look for the underlying
 * network signal: an {@link IOException}, a {@link RetriableConnectionException}, or a gRPC status
 * with a retryable code.
 *
 * <p>When offline, the SDK's native (JNI) {@code waitForOffset}/{@code ingestRecordsOffset} surface
 * a {@link ZerobusException} carrying only a message — gRPC status objects never cross the JNI
 * boundary, so none of the signals above are present. We therefore also treat any {@link
 * ZerobusException} as transient, since that is the SDK's own contract: {@link
 * NonRetriableException} is its "do not retry" marker (schema/auth/permanent), and the SDK
 * auto-recovers from every other {@code ZerobusException}. Anything we don't recognize is treated
 * as permanent.
 */
public class ZerobusConnectionFailureClassifier implements ConnectionFailureClassifier {

  // gRPC status codes that mean "transient, retry": the call never reached a definitive answer.
  private static final Set<String> RETRYABLE_GRPC_CODES =
      Set.of("UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED");

  @Override
  public boolean isConnectionFailure(Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      if (cause instanceof RetriableConnectionException || cause instanceof IOException) {
        return true;
      }
      // The SDK's own retriability signal. NonRetriableException is authoritative "permanent" — a
      // waitForOffset failure here means the batch may already be committed, but buffering+replay
      // (at-least-once, downstream dedupes) beats dropping it. NonRetriableException is checked
      // first since it IS-A ZerobusException.
      if (cause instanceof NonRetriableException) {
        return false;
      }
      if (cause instanceof ZerobusException) {
        return true;
      }
      if (isRetryableGrpcStatus(cause)) {
        return true;
      }
    }
    return false;
  }

  /**
   * gRPC's {@code StatusRuntimeException} is not on our compile classpath as a hard dependency, so
   * match it structurally: the class name ends with {@code StatusRuntimeException}/{@code
   * StatusException} and the message starts with a retryable status code (gRPC formats it as {@code
   * "UNAVAILABLE: ..."}).
   */
  private static boolean isRetryableGrpcStatus(Throwable cause) {
    String className = cause.getClass().getName();
    if (!className.endsWith("StatusRuntimeException") && !className.endsWith("StatusException")) {
      return false;
    }
    String message = cause.getMessage();
    if (message == null) {
      return false;
    }
    int colon = message.indexOf(':');
    String code = (colon >= 0 ? message.substring(0, colon) : message).trim();
    return RETRYABLE_GRPC_CODES.contains(code);
  }
}
