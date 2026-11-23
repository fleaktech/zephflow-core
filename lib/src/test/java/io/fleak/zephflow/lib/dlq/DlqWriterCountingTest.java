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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

class DlqWriterCountingTest {

  private final FleakCounter unwrittenDeadLetterCounter = mock(FleakCounter.class);

  @Test
  void deadLettersThatCannotBeSerializedAreCountedAndNotUploaded() {
    S3Client s3Client = mock(S3Client.class);
    S3DlqWriter writer =
        new S3DlqWriter(
            s3Client,
            "dlq",
            1,
            600_000,
            "prefix",
            "dead-letters",
            "deadletter",
            unwrittenDeadLetterCounter,
            S3DlqWriter.STORAGE_UPLOADS);

    writer.doWrite(new DeadLetter(1L, null, null, null, null, null));

    verify(unwrittenDeadLetterCounter).increase(1, Map.of());
    verifyNoInteractions(s3Client);
  }

  @Test
  void aGcsUploadThatKeepsFailingIsCounted() {
    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class)))
        .thenThrow(new StorageException(503, "unavailable"));
    GcsDlqWriter writer =
        new GcsDlqWriter(
            storage,
            "dlq",
            1,
            600_000,
            "prefix",
            "dead-letters",
            "deadletter",
            unwrittenDeadLetterCounter);

    writer.writeToDlq(1L, new SerializedEvent(null, "a".getBytes(), Map.of()), "error", "node-1");

    verify(unwrittenDeadLetterCounter).increase(1, Map.of());
  }

  @Test
  void storageAttemptTimeoutIsFifteenSecondsPlusThePayloadAtOneMegabytePerSecond() {
    Map<Long, Duration> timeouts =
        Stream.of(0L, 1_000_000L, 10_000_000L, 100_000_000L)
            .collect(Collectors.toMap(bytes -> bytes, S3DlqWriter::storageAttemptTimeout));

    assertEquals(
        Map.of(
            0L, Duration.ofSeconds(15),
            1_000_000L, Duration.ofSeconds(16),
            10_000_000L, Duration.ofSeconds(25),
            100_000_000L, Duration.ofSeconds(115)),
        timeouts);
  }
}
