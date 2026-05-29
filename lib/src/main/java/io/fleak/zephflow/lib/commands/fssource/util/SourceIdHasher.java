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
package io.fleak.zephflow.lib.commands.fssource.util;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

public final class SourceIdHasher {

  private SourceIdHasher() {}

  /**
   * Stable 16-hex-char id derived from the source-identity fields. Two configs with the same {@code
   * (backend, root, fileNameRegex)} share a sourceId — and therefore share checkpoints. Other
   * fields (mode, partition, emission, listingInterval, checkpoint override) do NOT participate.
   */
  public static String compute(String backend, String root, String fileNameRegex) {
    String canonical = backend + "\n" + root + "\n" + (fileNameRegex == null ? "" : fileNameRegex);
    return Hashing.sha256()
        .hashString(canonical, StandardCharsets.UTF_8)
        .toString()
        .substring(0, 16);
  }
}
