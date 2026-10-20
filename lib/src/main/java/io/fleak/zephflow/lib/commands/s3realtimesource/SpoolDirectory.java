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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import io.fleak.zephflow.lib.commands.filedecode.LocalSpool;
import java.nio.file.Path;

/** The directory one S3 realtime source instance downloads objects into ({@link LocalSpool}). */
final class SpoolDirectory implements AutoCloseable {

  private static final String PREFIX = "s3rt-spool-";

  private final LocalSpool spool;

  private SpoolDirectory(LocalSpool spool) {
    this.spool = spool;
  }

  static SpoolDirectory create() {
    return new SpoolDirectory(LocalSpool.create(PREFIX));
  }

  static SpoolDirectory create(Path workingDir, Path tmpDir) {
    return new SpoolDirectory(LocalSpool.create(PREFIX, workingDir, tmpDir));
  }

  Path getPath() {
    return spool.getPath();
  }

  @Override
  public void close() {
    spool.close();
  }
}
