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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import io.fleak.zephflow.lib.commands.sink.DestinationFailureClassifier;
import io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkFlusher.HecResponseException;
import java.util.Set;

/**
 * HEC rejects a whole batch with status 400 and one of these codes when an event in it is
 * malformed; that is a record problem. Every other failure, including ones it cannot classify,
 * counts as a destination problem.
 */
final class HecDestinationFailureClassifier implements DestinationFailureClassifier {

  private static final Set<Integer> RECORD_PROBLEM_CODES = Set.of(5, 6, 12, 13, 15);

  @Override
  public boolean isDestinationFailure(Throwable t) {
    return !(t instanceof HecResponseException hec
        && hec.getStatus() == 400
        && hec.getHecCode() != null
        && RECORD_PROBLEM_CODES.contains(hec.getHecCode()));
  }
}
