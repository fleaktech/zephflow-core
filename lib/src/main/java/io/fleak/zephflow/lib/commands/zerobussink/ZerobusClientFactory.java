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
package io.fleak.zephflow.lib.commands.zerobussink;

import com.databricks.zerobus.ZerobusSdk;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ZerobusClientFactory {

  /**
   * Creates a {@link ZerobusSdk}. The Zerobus SDK takes two endpoints: the gRPC ingest endpoint
   * (data plane) and the workspace / Unity Catalog endpoint (control plane, used for OAuth and
   * schema authorization). The latter comes from the Databricks credential's host.
   */
  public static ZerobusSdk createClient(String zerobusEndpoint, DatabricksCredential credential) {
    log.info(
        "Creating Zerobus SDK. ingestEndpoint={}, unityCatalogEndpoint={}",
        zerobusEndpoint,
        credential.getHost());
    return new ZerobusSdk(zerobusEndpoint, credential.getHost());
  }
}
