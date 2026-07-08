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

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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
}
