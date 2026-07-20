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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SftpBackendConfigTest {

  private static JobContext contextWithPasswordCred() {
    return JobContext.builder()
        .otherProperties(Map.of("cred1", new UsernamePasswordCredential("demo", "secret")))
        .build();
  }

  private static JobContext contextWithKeyCred() {
    return JobContext.builder()
        .otherProperties(Map.of("key1", new RSAPrivateKeyCredential("pkcs8-pem", "keyuser")))
        .build();
  }

  @Test
  void parsesHostAndDefaultPort() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload",
            Map.of("credentialId", "cred1"),
            contextWithPasswordCred());
    assertEquals("example.com", cfg.host());
    assertEquals(22, cfg.port());
  }

  @Test
  void parsesExplicitPort() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com:2222/upload",
            Map.of("credentialId", "cred1"),
            contextWithPasswordCred());
    assertEquals(2222, cfg.port());
  }

  @Test
  void passwordCredentialPopulatesUsernameAndPassword() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload",
            Map.of("credentialId", "cred1"),
            contextWithPasswordCred());
    assertEquals("demo", cfg.username());
    assertEquals("secret", cfg.password());
    assertNull(cfg.privateKeyPkcs8());
  }

  @Test
  void privateKeyCredentialPopulatesUsernameAndKey() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload",
            Map.of("privateKeyCredentialId", "key1"),
            contextWithKeyCred());
    assertEquals("keyuser", cfg.username());
    assertEquals("pkcs8-pem", cfg.privateKeyPkcs8());
    assertNull(cfg.password());
  }

  @Test
  void hostKeyFingerprintIsCarriedThrough() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload",
            Map.of("credentialId", "cred1", "hostKeyFingerprint", "SHA256:abc"),
            contextWithPasswordCred());
    assertEquals("SHA256:abc", cfg.hostKeyFingerprint());
  }

  @Test
  void bothCredentialKeysRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SftpBackendConfig.from(
                    "sftp://example.com/upload",
                    Map.of("credentialId", "cred1", "privateKeyCredentialId", "key1"),
                    contextWithPasswordCred()));
    assertTrue(e.getMessage().contains("exactly one"));
  }

  @Test
  void neitherCredentialKeyRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SftpBackendConfig.from(
                "sftp://example.com/upload", Map.of(), contextWithPasswordCred()));
  }

  @Test
  void nullBackendConfigMapRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SftpBackendConfig.from("sftp://example.com/upload", null, contextWithPasswordCred()));
  }

  @Test
  void invalidRootRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SftpBackendConfig.from(
                "/no/scheme/here", Map.of("credentialId", "cred1"), contextWithPasswordCred()));
  }
}
