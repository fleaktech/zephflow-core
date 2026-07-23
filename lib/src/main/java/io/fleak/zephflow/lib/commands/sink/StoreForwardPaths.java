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

import io.fleak.zephflow.api.JobContext;
import java.io.Serializable;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the on-disk store-and-forward buffer directory for a sink node.
 *
 * <p>The layout is always {@code <base>/<jobId>/<nodeId>/replica-<replicaIndex>} so that replicas
 * of the same job and different jobs on the same host never share a buffer folder, while a
 * restarted replica (same job id + replica index) finds its previous buffer and keeps draining it.
 *
 * <p>Base directory precedence: explicit {@code localStorePath} config, then the durable directory
 * injected by the scheduler under {@link JobContext#STORE_FORWARD_DIR}, then a tmpdir fallback for
 * local/SDK runs.
 */
@Slf4j
public final class StoreForwardPaths {

  static final String DEFAULT_BASE_DIR_NAME = "zephflow-store-forward";
  static final String METRIC_TAG_JOB_ID = "job_id";
  static final String DEFAULT_JOB_ID = "local";
  static final String REPLICA_DIR_PREFIX = "replica-";

  private StoreForwardPaths() {}

  public static Path resolve(String localStorePath, JobContext jobContext, String nodeId) {
    return baseDir(localStorePath, jobContext)
        .resolve(jobId(jobContext))
        .resolve(nodeId)
        .resolve(REPLICA_DIR_PREFIX + replicaIndex(jobContext));
  }

  static Path baseDir(String localStorePath, JobContext jobContext) {
    if (localStorePath != null && !localStorePath.isBlank()) {
      return Path.of(localStorePath);
    }
    Serializable injected =
        jobContext.getOtherProperties() == null
            ? null
            : jobContext.getOtherProperties().get(JobContext.STORE_FORWARD_DIR);
    if (injected != null && !injected.toString().isBlank()) {
      return Path.of(injected.toString());
    }
    return Path.of(System.getProperty("java.io.tmpdir"), DEFAULT_BASE_DIR_NAME);
  }

  private static String jobId(JobContext jobContext) {
    String jobId =
        jobContext.getMetricTags() == null
            ? null
            : jobContext.getMetricTags().get(METRIC_TAG_JOB_ID);
    if (jobId == null || jobId.isBlank()) {
      return DEFAULT_JOB_ID;
    }
    return jobId.replaceAll("[^a-zA-Z0-9._-]", "_");
  }

  private static int replicaIndex(JobContext jobContext) {
    Serializable value =
        jobContext.getOtherProperties() == null
            ? null
            : jobContext.getOtherProperties().get(JobContext.REPLICA_INDEX);
    if (value == null) {
      return 0;
    }
    try {
      return Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException e) {
      log.warn("store-and-forward: unparseable {}={}; using 0", JobContext.REPLICA_INDEX, value);
      return 0;
    }
  }
}
