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

import java.util.Objects;

/**
 * Stable identifier for a file across listings. {@code backend} matches the {@code
 * FsBackend.scheme()}.
 */
public record FileKey(String backend, String urn) {

  public FileKey {
    Objects.requireNonNull(backend, "backend");
    Objects.requireNonNull(urn, "urn");
  }

  /** Parse a URN like {@code s3://bucket/key} or {@code file:///abs/path}. */
  public static FileKey of(String urn) {
    Objects.requireNonNull(urn, "urn");
    int idx = urn.indexOf("://");
    if (idx <= 0) {
      throw new IllegalArgumentException("URN missing scheme: " + urn);
    }
    return new FileKey(urn.substring(0, idx), urn);
  }
}
