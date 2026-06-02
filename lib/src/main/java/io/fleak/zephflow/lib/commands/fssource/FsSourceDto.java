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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.api.CommandConfig;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface FsSourceDto {

  enum Mode {
    BOUNDED,
    UNBOUNDED
  }

  enum EmissionType {
    LINE,
    WHOLE_FILE,
    FILE_REFERENCE
  }

  enum PostActionType {
    NONE,
    DELETE,
    ARCHIVE
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  class Config implements CommandConfig {
    private String backend;

    private String root;

    private String fileNameRegex;

    @Builder.Default private Emission emission = Emission.builder().type(EmissionType.LINE).build();
    @Builder.Default private Mode mode = Mode.BOUNDED;
    @Builder.Default private long listingIntervalMs = 30_000;

    @Builder.Default private Stability stability = Stability.builder().enabled(false).build();

    @Builder.Default
    private PostActionConfig postAction =
        PostActionConfig.builder().type(PostActionType.NONE).build();

    private PartitionConfig partition;
    private CheckpointOverride checkpoint;

    private Map<String, Object> backendConfig;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Emission {
    private EmissionType type;
    @Builder.Default private String encoding = "utf-8";
    @Builder.Default private int lineBatchSize = 500;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Stability {
    private boolean enabled;
    @Builder.Default private long probeDelayMs = 10_000;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class PostActionConfig {
    private PostActionType type;

    private String destinationPrefix;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class PartitionConfig {
    private int index;
    private int parallelism;
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class CheckpointOverride {
    private String backend;
    private String root;
  }
}
