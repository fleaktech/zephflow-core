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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import java.io.Serializable;

/**
 * Wires the InfluxDB v2 client. Serializable and injectable so the command can substitute a stub in
 * unit tests, mirroring {@code AzureEventHubClientFactory}.
 */
public class InfluxDbClientProvider implements Serializable {

  /**
   * Creates a v2 client bound to a default org and bucket. A null/blank token connects without
   * authentication (local / unsecured InfluxDB).
   */
  public InfluxDBClient create(String url, String token, String org, String bucket) {
    char[] tokenChars = token == null ? new char[0] : token.toCharArray();
    return InfluxDBClientFactory.create(url, tokenChars, org, bucket);
  }
}
