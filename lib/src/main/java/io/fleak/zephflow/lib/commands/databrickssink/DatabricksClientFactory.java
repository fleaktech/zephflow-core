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
package io.fleak.zephflow.lib.commands.databrickssink;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatabricksClientFactory {

  public static WorkspaceClient createClient(DatabricksCredential credential) {
    log.info(
        "Creating Databricks WorkspaceClient with OAuth M2M for host: {}", credential.getHost());

    DatabricksConfig config =
        new DatabricksConfig()
            .setHost(credential.getHost())
            .setClientId(credential.getClientId())
            .setClientSecret(credential.getClientSecret());

    if (credential.getAzureTenantId() != null) {
      config.setAzureTenantId(credential.getAzureTenantId());
    }

    WorkspaceClient client = new WorkspaceClient(config);

    log.info("Databricks WorkspaceClient created successfully. OAuth tokens will auto-refresh.");

    return client;
  }
}
