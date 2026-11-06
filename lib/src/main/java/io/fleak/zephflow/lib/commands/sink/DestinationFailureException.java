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
package io.fleak.zephflow.lib.commands.sink;

import io.fleak.zephflow.api.ErrorOutput;
import java.util.List;
import lombok.Getter;

/**
 * A send that failed because of the destination, after the flusher already found records that fail
 * on their own content. Store-and-forward keeps the batch's other records and reports these errors.
 */
@Getter
public class DestinationFailureException extends RuntimeException {
  private final List<ErrorOutput> recordErrors;

  public DestinationFailureException(Throwable cause, List<ErrorOutput> recordErrors) {
    super(cause.getMessage(), cause);
    this.recordErrors = List.copyOf(recordErrors);
  }
}
