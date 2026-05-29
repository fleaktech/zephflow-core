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
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import java.util.List;
import java.util.Optional;

public interface CheckpointStore extends AutoCloseable {

  Optional<FsCheckpoint> load(String checkpointKey);

  /** Atomic single-writer PUT. Implementations MUST ensure readers never see a torn write. */
  void save(String checkpointKey, FsCheckpoint cp);

  /**
   * List the generations (parallelism values) for which any shard exists under {@code sourceId}.
   */
  List<Integer> listGenerations(String sourceId);

  /**
   * List the shard keys ({@code <sourceId>/<generation>/<jobIndex>.json}) for a given generation.
   */
  List<String> listShards(String sourceId, int generation);

  @Override
  default void close() {}
}
