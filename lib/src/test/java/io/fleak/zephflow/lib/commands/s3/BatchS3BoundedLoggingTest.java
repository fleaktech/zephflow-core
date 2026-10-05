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
package io.fleak.zephflow.lib.commands.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BatchS3BoundedLoggingTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cleanupFailureSuppressesRawExceptionOnlyForBoundedExecution(boolean bounded)
      throws Exception {
    var hooks = new ExecutionHooks(() -> {}, mock(ExecutionHooks.Effects.class), Runnable::run);
    var job = JobContext.builder().executionHooks(bounded ? hooks : null).build();
    var sink =
        new BatchS3Flusher(
            mock(AwsClientFactory.S3TransferResources.class),
            "bucket",
            "key",
            mock(),
            100,
            60000,
            null,
            job,
            "sink",
            mock(),
            mock(),
            mock());
    sink.initialize();
    var directory = BatchS3Flusher.class.getDeclaredField("tempDirectory");
    directory.setAccessible(true);
    var path = (java.nio.file.Path) directory.get(sink);
    var failure =
        new IOException(
            "token=s3-cleanup-private-token", new IllegalStateException("private-nested-value"));
    try (var logs = new BoundedLogCapture(BatchS3Flusher.class);
        var files = mockStatic(FileUtils.class)) {
      files.when(() -> FileUtils.deleteDirectory(path.toFile())).thenThrow(failure);
      sink.close();
      assertFalse(logs.events().isEmpty());
      if (bounded) {
        assertTrue(logs.events().stream().allMatch(event -> event.getThrown() == null));
        assertTrue(
            logs.events().stream()
                .noneMatch(
                    event -> event.getMessage().getFormattedMessage().contains("private-token")));
      } else {
        assertTrue(logs.events().stream().anyMatch(event -> event.getThrown() == failure));
      }
    } finally {
      java.nio.file.Files.deleteIfExists(path);
    }
  }
}
