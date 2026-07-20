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
package io.fleak.zephflow.lib.commands.kafkasink;

import io.fleak.zephflow.lib.commands.sink.ConnectionFailureClassifier;
import java.io.IOException;
import org.apache.kafka.common.errors.RetriableException;

/**
 * Classifies a Kafka send failure as transient connectivity (buffer + retry) versus permanent (drop
 * / normal error path). Kafka's own {@link RetriableException} hierarchy — {@code
 * TimeoutException}, {@code NetworkException}, etc. — is exactly "transient, retry", so we walk the
 * cause chain for it (or a plain {@link IOException}). Anything else is treated as permanent.
 */
public class KafkaConnectionFailureClassifier implements ConnectionFailureClassifier {

  @Override
  public boolean isConnectionFailure(Throwable t) {
    for (Throwable cause = t;
        cause != null && cause != cause.getCause();
        cause = cause.getCause()) {
      if (cause instanceof RetriableException || cause instanceof IOException) {
        return true;
      }
    }
    return false;
  }
}
