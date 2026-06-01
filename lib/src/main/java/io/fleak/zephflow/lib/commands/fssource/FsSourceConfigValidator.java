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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class FsSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig config, String nodeId, JobContext ctx) {
    if (!(config instanceof FsSourceDto.Config c)) {
      throw new IllegalArgumentException("expected FsSourceDto.Config, got " + config.getClass());
    }
    if (c.getBackend() == null || c.getBackend().isBlank()) {
      throw new IllegalArgumentException("backend is required");
    }
    if (c.getRoot() == null || c.getRoot().isBlank()) {
      throw new IllegalArgumentException("root is required");
    }
    // Don't require the backend to be registered at validate time (helps testing).
    if (c.getFileNameRegex() != null && !c.getFileNameRegex().isBlank()) {
      try {
        Pattern.compile(c.getFileNameRegex());
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("invalid fileNameRegex: " + e.getMessage(), e);
      }
    }
    if (c.getEmission() == null || c.getEmission().getType() == null) {
      throw new IllegalArgumentException("emission.type is required");
    }
    if (c.getEmission().getType() == FsSourceDto.EmissionType.LINE
        && c.getEmission().getLineBatchSize() <= 0) {
      throw new IllegalArgumentException("emission.lineBatchSize must be > 0");
    }
    if (c.getPartition() != null) {
      if (c.getPartition().getParallelism() <= 0) {
        throw new IllegalArgumentException("partition.parallelism must be > 0");
      }
      if (c.getPartition().getIndex() < 0
          || c.getPartition().getIndex() >= c.getPartition().getParallelism()) {
        throw new IllegalArgumentException("partition.index must be in [0, partition.parallelism)");
      }
    }
    if (c.getPostAction() != null
        && c.getPostAction().getType() == FsSourceDto.PostActionType.ARCHIVE
        && (c.getPostAction().getDestinationPrefix() == null
            || c.getPostAction().getDestinationPrefix().isBlank())) {
      throw new IllegalArgumentException("postAction.destinationPrefix required for ARCHIVE");
    }
  }
}
