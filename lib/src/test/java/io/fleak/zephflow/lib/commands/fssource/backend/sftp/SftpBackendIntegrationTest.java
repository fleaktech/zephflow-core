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

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.SecurityUtils;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Tag("integration")
@Testcontainers
class SftpBackendIntegrationTest {

  private static final int SFTP_PORT = 22;
  private static final String USER = "demo";
  private static final String PASSWORD = "secret";

  @Container
  static GenericContainer<?> SFTP =
      new GenericContainer<>(DockerImageName.parse("atmoz/sftp:alpine"))
          .withExposedPorts(SFTP_PORT)
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("sftp/test_key.pub"),
              "/home/" + USER + "/.ssh/keys/test_key.pub")
          .withCommand(USER + ":" + PASSWORD + ":1001::upload");

  @BeforeAll
  static void seedFiles() throws Exception {
    exec("mkdir -p /home/demo/upload/data/nested");
    copy("hello", "/home/demo/upload/data/evt_1.log");
    copy("world", "/home/demo/upload/data/evt_2.log");
    copy("nope", "/home/demo/upload/data/skip.txt");
    copy("deep", "/home/demo/upload/data/nested/evt_3.log");
  }

  private static void exec(String cmd) throws Exception {
    var result = SFTP.execInContainer("sh", "-c", cmd);
    assertEquals(0, result.getExitCode(), result.getStderr());
  }

  private static void copy(String content, String containerPath) {
    SFTP.copyFileToContainer(
        Transferable.of(content.getBytes(StandardCharsets.UTF_8)), containerPath);
  }

  private static SftpBackendConfig passwordConfig() {
    return new SftpBackendConfig(
        SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, PASSWORD, null, null);
  }

  private static String rootUrn() {
    return "sftp://" + SFTP.getHost() + ":" + SFTP.getMappedPort(SFTP_PORT) + "/upload/data";
  }

  @Test
  void connectsAndStatsWithPasswordAuth() throws Exception {
    try (SftpConnection connection = new SftpConnection(passwordConfig())) {
      assertNotNull(connection.sftp().stat("/upload/data/evt_1.log"));
    }
  }

  @Test
  void failsWithWrongPassword() {
    SftpBackendConfig bad =
        new SftpBackendConfig(
            SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, "wrong-password", null, null);
    try (SftpConnection connection = new SftpConnection(bad)) {
      assertThrows(UncheckedIOException.class, connection::sftp);
    }
  }

  @Test
  void closeIsIdempotent() {
    SftpConnection connection = new SftpConnection(passwordConfig());
    connection.sftp();
    connection.close();
    connection.close();
  }

  @Test
  void listsRecursivelyWithRegexFilter() {
    try (SftpLister lister = new SftpLister(passwordConfig())) {
      List<FileEntry> entries =
          lister.list(new ListRequest(rootUrn(), Pattern.compile("evt_\\d+\\.log"))).toList();

      assertEquals(3, entries.size()); // evt_1, evt_2, nested/evt_3; skip.txt filtered out
      assertTrue(entries.stream().allMatch(e -> e.key().backend().equals("sftp")));
      assertTrue(
          entries.stream().anyMatch(e -> e.key().urn().endsWith("/upload/data/nested/evt_3.log")));
      assertTrue(entries.stream().allMatch(e -> e.key().urn().startsWith("sftp://")));
      assertTrue(entries.stream().allMatch(e -> e.size() > 0));
      assertTrue(entries.stream().allMatch(e -> e.lastModified().isAfter(Instant.EPOCH)));
    }
  }

  @Test
  void listsEverythingWithoutRegex() {
    try (SftpLister lister = new SftpLister(passwordConfig())) {
      List<FileEntry> entries = lister.list(new ListRequest(rootUrn(), null)).toList();
      assertEquals(4, entries.size()); // includes skip.txt
    }
  }

  @Test
  void statReturnsEntry() {
    try (SftpLister lister = new SftpLister(passwordConfig())) {
      String urn = rootUrn() + "/evt_1.log";
      FileEntry entry = lister.stat(FileKey.of(urn));
      assertEquals(5, entry.size()); // "hello"
      assertEquals(urn, entry.key().urn());
    }
  }

  @Test
  void readsFullFile() throws Exception {
    try (SftpReader reader = new SftpReader(passwordConfig())) {
      try (InputStream in = reader.open(FileKey.of(rootUrn() + "/evt_1.log"), 0)) {
        assertEquals("hello", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  void readsFromOffset() throws Exception {
    try (SftpReader reader = new SftpReader(passwordConfig())) {
      try (InputStream in = reader.open(FileKey.of(rootUrn() + "/evt_1.log"), 2)) {
        assertEquals("llo", new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  void openingMissingFileThrows() {
    try (SftpReader reader = new SftpReader(passwordConfig())) {
      assertThrows(
          UncheckedIOException.class,
          () -> reader.open(FileKey.of(rootUrn() + "/does_not_exist.log"), 0));
    }
  }

  private static String testKeyPkcs8() throws IOException {
    try (InputStream in =
        SftpBackendIntegrationTest.class.getResourceAsStream("/sftp/test_key_pkcs8.pem")) {
      return new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** Connects once with an accept-all verifier that records the server's key fingerprint. */
  private static String captureHostKeyFingerprint() throws IOException {
    AtomicReference<String> fingerprint = new AtomicReference<>();
    try (SSHClient probe = new SSHClient()) {
      probe.addHostKeyVerifier(
          new HostKeyVerifier() {
            @Override
            public boolean verify(String hostname, int port, PublicKey key) {
              fingerprint.set(SecurityUtils.getFingerprint(key));
              return true;
            }

            @Override
            public List<String> findExistingAlgorithms(String hostname, int port) {
              return List.of();
            }
          });
      probe.connect(SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT));
    }
    return fingerprint.get();
  }

  @Test
  void connectsWithPrivateKeyAuth() throws Exception {
    SftpBackendConfig cfg =
        new SftpBackendConfig(
            SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, null, testKeyPkcs8(), null);
    try (SftpConnection connection = new SftpConnection(cfg)) {
      assertNotNull(connection.sftp().stat("/upload/data/evt_1.log"));
    }
  }

  @Test
  void correctHostKeyFingerprintConnects() throws Exception {
    String fingerprint = captureHostKeyFingerprint();
    SftpBackendConfig cfg =
        new SftpBackendConfig(
            SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, PASSWORD, null, fingerprint);
    try (SftpConnection connection = new SftpConnection(cfg)) {
      assertNotNull(connection.sftp().stat("/upload/data/evt_1.log"));
    }
  }

  @Test
  void wrongHostKeyFingerprintFails() {
    // Valid MD5 fingerprint format, wrong value.
    String wrong = "00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff";
    SftpBackendConfig cfg =
        new SftpBackendConfig(
            SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, PASSWORD, null, wrong);
    try (SftpConnection connection = new SftpConnection(cfg)) {
      assertThrows(UncheckedIOException.class, connection::sftp);
    }
  }
}
