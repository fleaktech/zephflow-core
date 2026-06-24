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

  public static String compute(String backend, String root, String fileNameRegex) {
    return hash16(canonical(backend, root, fileNameRegex));
  }

  public static String compute(
      String backend, String root, String fileNameRegex, int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) {
      return compute(backend, root, fileNameRegex);
    }
    return hash16(
        canonical(backend, root, fileNameRegex) + "\n" + replicaIndex + "\n" + replicaCount);
  }

  private static String canonical(String backend, String root, String fileNameRegex) {
    return backend + "\n" + root + "\n" + (fileNameRegex == null ? "" : fileNameRegex);
  }

  private static String hash16(String value) {
    return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString().substring(0, 16);
  }
}
