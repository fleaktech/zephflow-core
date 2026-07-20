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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendConfig;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@code FsSourceCommand.buildBackendConfig}'s {@code "sftp"} arm end-to-end via the
 * public {@code parseAndValidateArg -> initialize} wiring (mirrors how {@code
 * S3RealtimeSourceCommandTest} drives the factory -> parse -> validate -> createExecutionContext
 * path for another backend).
 *
 * <p>No real SFTP server is required: {@code SftpLister}/{@code SftpReader} wrap a lazily
 * established {@code SftpConnection} that only opens a network connection on first use (see {@code
 * SftpConnection.sftp()}), so simply building the execution context never touches the network.
 */
class FsSourceCommandSftpBackendConfigTest {

  @Test
  void sftpBackendConfigIsBuiltFromRootAndCredential() throws Exception {
    JobContext jobContext =
        JobContext.builder()
            .otherProperties(
                new HashMap<>(Map.of("cred1", new UsernamePasswordCredential("demo", "secret"))))
            .build();

    FsSourceCommand command =
        (FsSourceCommand) new FsSourceCommandFactory().createCommand("node", jobContext);

    Map<String, Object> config =
        Map.of(
            "backend", "sftp",
            "root", "sftp://example.com:2222/upload",
            "encodingType", "JSON_OBJECT_LINE",
            "backendConfig", Map.of("credentialId", "cred1"));
    command.parseAndValidateArg(config);
    command.initialize(mock(MetricClientProvider.class));

    FsSourceExecutionContext executionContext =
        (FsSourceExecutionContext) command.getExecutionContext();
    assertInstanceOf(SftpBackendConfig.class, executionContext.backendConfig);
    SftpBackendConfig sftpBackendConfig = (SftpBackendConfig) executionContext.backendConfig;
    assertEquals("example.com", sftpBackendConfig.host());
    assertEquals(2222, sftpBackendConfig.port());
    assertEquals("demo", sftpBackendConfig.username());
    assertEquals("secret", sftpBackendConfig.password());

    command.terminate();
  }
}
