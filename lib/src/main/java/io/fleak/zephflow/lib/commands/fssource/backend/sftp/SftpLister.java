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
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;

public final class SftpLister implements FileLister {

  private final SftpBackendConfig cfg;
  private final SftpConnection connection;

  public SftpLister(SftpBackendConfig cfg) {
    this.cfg = cfg;
    this.connection = new SftpConnection(cfg);
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    String rootPath = URI.create(req.root()).getPath();
    if (rootPath == null || rootPath.isEmpty()) {
      rootPath = "/";
    }
    SFTPClient sftp = connection.sftp();
    // Eager collection: FsSourceCommand drains the stream immediately, and eager
    // traversal keeps connection state simple.
    List<FileEntry> entries = new ArrayList<>();
    Deque<String> dirs = new ArrayDeque<>();
    dirs.push(rootPath);
    try {
      while (!dirs.isEmpty()) {
        String dir = dirs.pop();
        for (RemoteResourceInfo info : sftp.ls(dir)) {
          // Symlinks report type SYMLINK: neither isDirectory() nor isRegularFile()
          // is true, so they are skipped (no cycle risk).
          if (info.isDirectory()) {
            dirs.push(info.getPath());
          } else if (info.isRegularFile()
              && (req.fileNameRegex() == null
                  || req.fileNameRegex().matcher(info.getName()).matches())) {
            entries.add(toEntry(info));
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to list " + req.root(), e);
    }
    return entries.stream();
  }

  @Override
  public FileEntry stat(FileKey key) {
    String path = URI.create(key.urn()).getPath();
    try {
      FileAttributes attrs = connection.sftp().stat(path);
      return new FileEntry(
          key, attrs.getSize(), Instant.ofEpochSecond(attrs.getMtime()), key.urn());
    } catch (IOException e) {
      throw new UncheckedIOException("failed to stat " + key.urn(), e);
    }
  }

  @Override
  public void close() {
    connection.close();
  }

  private FileEntry toEntry(RemoteResourceInfo info) {
    String urn = "sftp://" + cfg.host() + ":" + cfg.port() + info.getPath();
    return new FileEntry(
        new FileKey(SftpBackendConfig.SCHEME, urn),
        info.getAttributes().getSize(),
        Instant.ofEpochSecond(info.getAttributes().getMtime()),
        urn);
  }
}
