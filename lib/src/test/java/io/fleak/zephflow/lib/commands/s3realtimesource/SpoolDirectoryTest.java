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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpoolDirectoryTest {

  @TempDir Path workingDir;
  @TempDir Path tmpDir;

  @Test
  void isCreatedUnderTheWorkingDirectory() {
    try (SpoolDirectory spool = SpoolDirectory.create(workingDir, tmpDir)) {
      assertEquals(workingDir, spool.getPath().getParent());
      assertTrue(Files.isDirectory(spool.getPath()));
    }
  }

  @Test
  void fallsBackToTheTmpDirWhenTheWorkingDirectoryIsNotUsable() throws IOException {
    Path notADirectory = Files.writeString(workingDir.resolve("file"), "x");

    try (SpoolDirectory spool = SpoolDirectory.create(notADirectory, tmpDir)) {
      assertEquals(tmpDir, spool.getPath().getParent());
      assertTrue(Files.isDirectory(spool.getPath()));
    }
  }

  @Test
  void closeDeletesTheDirectoryWithItsFiles() throws IOException {
    SpoolDirectory spool = SpoolDirectory.create(workingDir, tmpDir);
    Files.writeString(spool.getPath().resolve("object-1.part"), "data");

    spool.close();
    spool.close();

    assertFalse(Files.exists(spool.getPath()));
  }
}
