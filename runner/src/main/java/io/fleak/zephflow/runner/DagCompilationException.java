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
package io.fleak.zephflow.runner;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Created by bolei on 4/10/25 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class DagCompilationException extends RuntimeException {
  private final String nodeId;
  private final String commandName;

  public DagCompilationException(
      String nodeId, String commandName, String message, Exception cause) {
    super(message, cause);
    this.nodeId = nodeId;
    this.commandName = commandName;
  }
}
