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

import java.util.stream.Stream;

/**
 * Per-backend listing + stat. Implementations MUST return a lazy stream that paginates under the
 * hood — the framework relies on this to avoid materializing entire buckets/directories.
 */
public interface FileLister extends AutoCloseable {

  /** Lazy, paginated iteration over all matching files under the given root. */
  Stream<FileEntry> list(ListRequest req);

  /** Re-stat a single file. Used by StabilityProbe. */
  FileEntry stat(FileKey key);

  @Override
  default void close() {}
}
