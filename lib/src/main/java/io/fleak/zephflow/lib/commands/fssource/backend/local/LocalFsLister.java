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
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class LocalFsLister implements FileLister {

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    Path root = toPath(req.root());
    Pattern regex = req.fileNameRegex();
    try {
      return Files.walk(root)
          .filter(Files::isRegularFile)
          .filter(p -> regex == null || regex.matcher(p.getFileName().toString()).matches())
          .map(LocalFsLister::toEntry);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public FileEntry stat(FileKey key) {
    Path p = Paths.get(java.net.URI.create(key.urn()));
    return toEntry(p);
  }

  /** Accept both plain paths (/abs/path) and file:// URIs (file:///abs/path). */
  private static Path toPath(String root) {
    if (root.startsWith("file:")) {
      return Paths.get(java.net.URI.create(root));
    }
    return Paths.get(root);
  }

  private static FileEntry toEntry(Path p) {
    try {
      BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class);
      FileKey k = new FileKey("file", p.toUri().toString());
      return new FileEntry(
          k, a.size(), Instant.ofEpochMilli(a.lastModifiedTime().toMillis()), p.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
