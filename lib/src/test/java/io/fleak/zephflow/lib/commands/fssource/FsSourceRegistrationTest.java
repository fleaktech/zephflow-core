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

import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FsSourceRegistrationTest {

  @BeforeEach
  void ensureLocalBackendRegistered() {
    // First ensure OperatorCommandRegistry is initialized (idempotent - only happens once)
    OperatorCommandRegistry.OPERATOR_COMMANDS.size();
    // Re-register LocalFsBackend if it was unregistered by another test
    try {
      FsBackendRegistry.get("file");
    } catch (IllegalArgumentException ignored) {
      FsBackendRegistry.register(
          new io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend());
    }
  }

  @Test
  void fsSourceIsRegistered() {
    // Access OPERATOR_COMMANDS to verify it's registered
    assertNotNull(OperatorCommandRegistry.OPERATOR_COMMANDS.get("fssource"));
  }

  @Test
  void localFsBackendIsRegistered() {
    // Verify the backend is registered (ensured by @BeforeEach)
    assertNotNull(FsBackendRegistry.get("file"));
  }
}
