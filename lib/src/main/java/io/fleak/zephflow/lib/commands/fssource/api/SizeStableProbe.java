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
package io.fleak.zephflow.lib.commands.fssource.api;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Default stability probe. Sees a file twice — if size and lastModified are unchanged, it is
 * stable.
 */
public final class SizeStableProbe implements StabilityProbe {

  private final Duration probeDelay;
  private final Map<FileKey, ProbeState> seen = new HashMap<>();

  public SizeStableProbe(Duration probeDelay) {
    this.probeDelay = probeDelay;
  }

  @Override
  public boolean isStable(FileEntry file, FileLister lister) {
    ProbeState prior = seen.get(file.key());
    Instant now = Instant.now();
    if (prior == null) {
      seen.put(file.key(), new ProbeState(file.size(), file.lastModified(), now));
      return false;
    }
    if (Duration.between(prior.firstSeenAt, now).compareTo(probeDelay) < 0) {
      return false;
    }
    FileEntry current = lister.stat(file.key());
    if (current.size() == prior.size && current.lastModified().equals(prior.lastModified)) {
      seen.remove(file.key());
      return true;
    }
    seen.put(file.key(), new ProbeState(current.size(), current.lastModified(), now));
    return false;
  }

  private record ProbeState(long size, Instant lastModified, Instant firstSeenAt) {}
}
