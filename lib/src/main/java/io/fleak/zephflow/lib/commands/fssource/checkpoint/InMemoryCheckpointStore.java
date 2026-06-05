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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryCheckpointStore implements CheckpointStore {

  private final ConcurrentMap<String, FsCheckpoint> store = new ConcurrentHashMap<>();

  @Override
  public Optional<FsCheckpoint> load(String checkpointKey) {
    return Optional.ofNullable(store.get(checkpointKey));
  }

  @Override
  public void save(String checkpointKey, FsCheckpoint cp) {
    store.put(checkpointKey, cp);
  }

  @Override
  public List<Integer> listGenerations(String sourceId) {
    Set<Integer> gens = new HashSet<>();
    String prefix = sourceId + "/";
    for (String key : store.keySet()) {
      if (!key.startsWith(prefix)) continue;
      String rest = key.substring(prefix.length());
      int slash = rest.indexOf('/');
      if (slash <= 0) continue;
      try {
        gens.add(Integer.parseInt(rest.substring(0, slash)));
      } catch (NumberFormatException ignored) {
      }
    }
    return new ArrayList<>(gens);
  }

  @Override
  public List<String> listShards(String sourceId, int generation) {
    String prefix = sourceId + "/" + generation + "/";
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, FsCheckpoint> e : store.entrySet()) {
      if (e.getKey().startsWith(prefix)) out.add(e.getKey());
    }
    return out;
  }
}
