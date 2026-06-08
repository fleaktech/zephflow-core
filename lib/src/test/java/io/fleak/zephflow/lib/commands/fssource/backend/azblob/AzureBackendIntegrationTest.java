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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class AzureBackendIntegrationTest {

  @Test
  void configHoldsConnectionString() {
    AzureBackendConfig cfg = new AzureBackendConfig("DefaultEndpoints=...", null, null);
    assertEquals("DefaultEndpoints=...", cfg.connectionString());
    assertNull(cfg.accountName());
    assertNull(cfg.accountKey());
  }

  @Test
  void configHoldsAccountKey() {
    AzureBackendConfig cfg = new AzureBackendConfig(null, "myaccount", "mykey");
    assertNull(cfg.connectionString());
    assertEquals("myaccount", cfg.accountName());
    assertEquals("mykey", cfg.accountKey());
  }
}
