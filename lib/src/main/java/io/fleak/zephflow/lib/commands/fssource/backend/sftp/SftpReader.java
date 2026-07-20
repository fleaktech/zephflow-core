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

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import net.schmizz.sshj.sftp.RemoteFile;

public final class SftpReader implements FileReader {

  private final SftpConnection connection;

  public SftpReader(SftpBackendConfig cfg) {
    this.connection = new SftpConnection(cfg);
  }

  @Override
  public InputStream open(FileKey key, long offset) {
    String path = URI.create(key.urn()).getPath();
    try {
      RemoteFile remoteFile = connection.sftp().open(path);
      InputStream in = remoteFile.new RemoteFileInputStream(offset);
      // Closing the returned stream must also release the remote file handle.
      return new FilterInputStream(in) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            remoteFile.close();
          }
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException("failed to open " + key.urn(), e);
    }
  }

  @Override
  public void close() {
    connection.close();
  }
}
