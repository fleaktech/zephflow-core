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
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FsCheckpointTest {

  @Test
  void withoutACeilingTheWatermarkFollowsTheEmittedTimestamp() {
    FsCheckpoint checkpoint =
        FsCheckpoint.empty().withEmitted("s3://b/a", Instant.ofEpochSecond(10), Instant.MAX);

    assertEquals(Instant.ofEpochSecond(10), checkpoint.watermark());
    assertTrue(checkpoint.isCompleted("s3://b/a"));
  }

  @Test
  void aCeilingHoldsTheWatermarkAtTheOldestUnresolvedFile() {
    Instant ceiling = Instant.ofEpochSecond(5);

    FsCheckpoint checkpoint =
        FsCheckpoint.empty().withEmitted("s3://b/later", Instant.ofEpochSecond(10), ceiling);

    assertEquals(
        ceiling,
        checkpoint.watermark(),
        "the watermark must not pass a file this run failed to resolve");
  }

  @Test
  void aHeldWatermarkStillRemembersTheFilesAlreadyEmittedAboveIt() {
    Instant ceiling = Instant.ofEpochSecond(5);

    FsCheckpoint checkpoint =
        FsCheckpoint.empty()
            .withEmitted("s3://b/one", Instant.ofEpochSecond(10), ceiling)
            .withEmitted("s3://b/two", Instant.ofEpochSecond(11), ceiling);

    assertEquals(ceiling, checkpoint.watermark());
    assertTrue(checkpoint.isCompleted("s3://b/one"), "a re-listed file must not be re-emitted");
    assertTrue(checkpoint.isCompleted("s3://b/two"));
  }

  @Test
  void theWatermarkNeverMovesBackwards() {
    FsCheckpoint advanced = new FsCheckpoint(1, Instant.ofEpochSecond(100), Map.of());

    FsCheckpoint checkpoint =
        advanced.withEmitted("s3://b/old", Instant.ofEpochSecond(3), Instant.ofEpochSecond(2));

    assertEquals(
        Instant.ofEpochSecond(100),
        checkpoint.watermark(),
        "a ceiling below the existing watermark must not rewind it");
  }
}
