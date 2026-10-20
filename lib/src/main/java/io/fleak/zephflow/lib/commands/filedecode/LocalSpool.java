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
package io.fleak.zephflow.lib.commands.filedecode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.file.PathUtils;

/**
 * The directory one source instance downloads files into. It lives under the JVM's working
 * directory, which in grid is the task's directory on the worker's data volume (deleted by grid
 * when the task exits), and falls back to {@code java.io.tmpdir} when that is not writable.
 */
@Slf4j
public final class LocalSpool implements AutoCloseable {

  @Getter private final Path path;

  private LocalSpool(Path path) {
    this.path = path;
  }

  /** A new directory named {@code prefix} and a random suffix. */
  public static LocalSpool create(String prefix) {
    return create(
        prefix,
        Path.of(System.getProperty("user.dir")),
        Path.of(System.getProperty("java.io.tmpdir")));
  }

  public static LocalSpool create(String prefix, Path workingDir, Path tmpDir) {
    try {
      return new LocalSpool(Files.createTempDirectory(workingDir, prefix));
    } catch (IOException | RuntimeException e) {
      log.warn("cannot create the spool directory under {}; using {}", workingDir, tmpDir, e);
    }
    try {
      return new LocalSpool(Files.createTempDirectory(tmpDir, prefix));
    } catch (IOException e) {
      throw new UncheckedIOException("cannot create the spool directory under " + tmpDir, e);
    }
  }

  @Override
  public void close() {
    if (!Files.exists(path)) {
      return;
    }
    try {
      PathUtils.deleteDirectory(path);
    } catch (IOException e) {
      log.warn("failed to delete the spool directory {}", path, e);
    }
  }
}
