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

import java.io.IOException;
import java.io.UncheckedIOException;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.FingerprintVerifier;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;

/**
 * One lazily-established SFTP session. Reconnects once per call if the underlying SSH transport has
 * dropped. Not thread-safe beyond the synchronized accessor; each lister/reader owns its own
 * instance.
 */
final class SftpConnection implements AutoCloseable {

  private final SftpBackendConfig cfg;
  private SSHClient ssh;
  private SFTPClient sftp;

  SftpConnection(SftpBackendConfig cfg) {
    this.cfg = cfg;
  }

  synchronized SFTPClient sftp() {
    if (ssh != null && ssh.isConnected()) {
      return sftp;
    }
    closeQuietly();
    try {
      ssh = new SSHClient();
      if (cfg.hostKeyFingerprint() != null && !cfg.hostKeyFingerprint().isBlank()) {
        ssh.addHostKeyVerifier(FingerprintVerifier.getInstance(cfg.hostKeyFingerprint()));
      } else {
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
      }
      ssh.connect(cfg.host(), cfg.port());
      authenticate();
      sftp = ssh.newSFTPClient();
      return sftp;
    } catch (IOException e) {
      closeQuietly();
      throw new UncheckedIOException(
          "failed to establish SFTP session to " + cfg.host() + ":" + cfg.port(), e);
    }
  }

  private void authenticate() throws IOException {
    if (cfg.password() != null) {
      ssh.authPassword(cfg.username(), cfg.password());
      return;
    }
    KeyProvider keyProvider = ssh.loadKeys(cfg.privateKeyPkcs8(), null, null);
    ssh.authPublickey(cfg.username(), keyProvider);
  }

  @Override
  public synchronized void close() {
    closeQuietly();
  }

  private void closeQuietly() {
    if (sftp != null) {
      try {
        sftp.close();
      } catch (IOException ignored) {
      }
      sftp = null;
    }
    if (ssh != null) {
      try {
        ssh.close();
      } catch (IOException ignored) {
      }
      ssh = null;
    }
  }
}
