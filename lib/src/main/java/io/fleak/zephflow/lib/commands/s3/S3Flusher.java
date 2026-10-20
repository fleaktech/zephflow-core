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
package io.fleak.zephflow.lib.commands.s3;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3Flusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  final S3Commiter<RecordFleakData> s3Commiter;

  public S3Flusher(S3Commiter<RecordFleakData> s3Commiter) {
    this.s3Commiter = s3Commiter;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<RecordFleakData> writable = new ArrayList<>();
    List<ErrorOutput> errors = new ArrayList<>();
    for (RecordFleakData event : preparedInputEvents.preparedList()) {
      try {
        s3Commiter.validate(event);
        writable.add(event);
      } catch (Exception e) {
        errors.add(new ErrorOutput(event, e.getMessage()));
      }
    }
    if (writable.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
    long size = s3Commiter.commit(writable);
    return new SimpleSinkCommand.FlushResult(writable.size(), size, errors);
  }

  @Override
  public void close() throws IOException {
    s3Commiter.close();
  }
}
