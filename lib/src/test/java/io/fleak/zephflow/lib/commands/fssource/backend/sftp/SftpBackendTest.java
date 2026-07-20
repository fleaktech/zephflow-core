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

import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SftpBackendTest {

  @Test
  void schemeAndCapabilities() {
    SftpBackend backend = new SftpBackend();
    assertEquals("sftp", backend.scheme());
    assertEquals(
        Set.of(
            FsBackend.Capability.DELETE,
            FsBackend.Capability.MOVE,
            FsBackend.Capability.RANGE_READ),
        backend.capabilities());
  }

  @Test
  void createsListerAndReader() {
    SftpBackend backend = new SftpBackend();
    SftpBackendConfig cfg = new SftpBackendConfig("example.com", 22, "u", "p", null, null);
    // No I/O happens until first use, so creation must succeed without a server.
    assertNotNull(backend.createLister(cfg));
    assertNotNull(backend.createReader(cfg));
  }
}
