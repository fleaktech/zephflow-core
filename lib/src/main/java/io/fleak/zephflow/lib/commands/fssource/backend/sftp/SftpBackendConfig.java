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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.net.URI;
import java.util.Map;

public record SftpBackendConfig(
    String host,
    int port,
    String username,
    String password,
    String privateKeyPkcs8,
    String hostKeyFingerprint)
    implements FsBackendConfig {

  public static final String SCHEME = "sftp";
  public static final int DEFAULT_PORT = 22;

  public static SftpBackendConfig from(
      String root, Map<String, Object> backendConfigMap, JobContext jobContext) {
    URI uri = URI.create(root);
    if (!SCHEME.equals(uri.getScheme()) || uri.getHost() == null) {
      throw new IllegalArgumentException(
          "sftp root must be of the form sftp://host[:port]/path, got: " + root);
    }
    String host = uri.getHost();
    int port = uri.getPort() < 0 ? DEFAULT_PORT : uri.getPort();
    if (backendConfigMap == null) backendConfigMap = Map.of();
    String credentialId = (String) backendConfigMap.get("credentialId");
    String privateKeyCredentialId = (String) backendConfigMap.get("privateKeyCredentialId");
    String hostKeyFingerprint = (String) backendConfigMap.get("hostKeyFingerprint");
    boolean hasPassword = credentialId != null && !credentialId.isBlank();
    boolean hasKey = privateKeyCredentialId != null && !privateKeyCredentialId.isBlank();
    if (hasPassword == hasKey) {
      throw new IllegalArgumentException(
          "sftp backend requires exactly one of 'credentialId' or 'privateKeyCredentialId' in backendConfig");
    }
    if (hasPassword) {
      UsernamePasswordCredential credential =
          MiscUtils.lookupUsernamePasswordCredential(jobContext, credentialId);
      return new SftpBackendConfig(
          host, port, credential.getUsername(), credential.getPassword(), null, hostKeyFingerprint);
    }
    RSAPrivateKeyCredential credential =
        MiscUtils.lookupRSAPrivateKeyCredential(jobContext, privateKeyCredentialId);
    return new SftpBackendConfig(
        host, port, credential.getUser(), null, credential.getKey(), hostKeyFingerprint);
  }
}
