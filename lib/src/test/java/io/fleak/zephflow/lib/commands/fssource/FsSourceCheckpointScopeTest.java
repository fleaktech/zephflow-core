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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.JobContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FsSourceCheckpointScopeTest {

  private static JobContext jobContext(String checkpointScope, String jobId) {
    Map<String, java.io.Serializable> otherProperties = new HashMap<>();
    if (checkpointScope != null) {
      otherProperties.put(JobContext.CHECKPOINT_SCOPE, checkpointScope);
    }
    Map<String, String> metricTags = new HashMap<>();
    if (jobId != null) {
      metricTags.put("job_id", jobId);
    }
    return JobContext.builder().otherProperties(otherProperties).metricTags(metricTags).build();
  }

  @Test
  void explicitScopeWinsOverJobId() {
    assertEquals("pipeline-1", FsSourceCommand.checkpointScope(jobContext("pipeline-1", "job-9")));
  }

  @Test
  void fallsBackToJobIdWhenNoExplicitScope() {
    assertEquals("job-9", FsSourceCommand.checkpointScope(jobContext(null, "job-9")));
  }

  @Test
  void blankExplicitScopeFallsBackToJobId() {
    assertEquals("job-9", FsSourceCommand.checkpointScope(jobContext("   ", "job-9")));
  }

  @Test
  void fallsBackToLocalWhenNothingIdentifiesTheJob() {
    assertEquals("local", FsSourceCommand.checkpointScope(jobContext(null, null)));
  }

  @Test
  void toleratesAbsentJobContextMaps() {
    JobContext withoutMaps = JobContext.builder().otherProperties(null).metricTags(null).build();

    assertEquals("local", FsSourceCommand.checkpointScope(withoutMaps));
  }
}
