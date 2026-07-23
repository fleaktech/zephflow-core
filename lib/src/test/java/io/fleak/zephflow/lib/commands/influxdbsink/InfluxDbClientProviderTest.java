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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static org.junit.jupiter.api.Assertions.*;

import com.influxdb.client.InfluxDBClient;
import org.junit.jupiter.api.Test;

class InfluxDbClientProviderTest {

  private final InfluxDbClientProvider provider = new InfluxDbClientProvider();

  @Test
  void createsClientWithToken() {
    try (InfluxDBClient client =
        provider.create("http://localhost:8086", "a-token", "org", "bucket")) {
      assertNotNull(client);
    }
  }

  @Test
  void createsClientWithoutTokenForUnauthenticatedAccess() {
    try (InfluxDBClient client = provider.create("http://localhost:8086", null, "org", "bucket")) {
      assertNotNull(client);
    }
  }
}
