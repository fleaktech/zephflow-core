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
package io.fleak.zephflow.lib.dlq;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingDlqWriter extends DlqWriter {

  public static LoggingDlqWriter create(JobContext.LoggingDlqConfig loggingDlqConfig) {
    return new LoggingDlqWriter();
  }

  @Override
  public void open() {}

  @Override
  protected void doWrite(DeadLetter deadLetter) {
    log.error("event error: {}", deadLetter);
  }

  @Override
  public void close() {}
}
