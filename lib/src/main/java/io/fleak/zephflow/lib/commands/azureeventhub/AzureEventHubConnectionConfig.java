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
package io.fleak.zephflow.lib.commands.azureeventhub;

import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;

/**
 * Connection and authentication parameters shared by the Event Hub source and sink. Decoupled from
 * the connector DTOs so {@link AzureEventHubClientFactory} can be exercised in isolation.
 *
 * <p>Exactly one authentication mode is expected:
 *
 * <ul>
 *   <li><b>SAS connection string</b> — {@link #connectionString} is set.
 *   <li><b>Entra ID (Azure AD)</b> — {@link #connectionString} is blank and {@link
 *       #fullyQualifiedNamespace} is set. When an explicit service principal ({@link #tenantId} +
 *       {@link #clientId} + {@link #clientSecret}) is supplied it is used, otherwise the ambient
 *       {@code DefaultAzureCredential} chain (managed identity, environment, CLI, ...) is used.
 * </ul>
 */
public record AzureEventHubConnectionConfig(
    String connectionString,
    String fullyQualifiedNamespace,
    String eventHubName,
    String tenantId,
    String clientId,
    String clientSecret)
    implements Serializable {

  /** True when a SAS connection string is used; false when Entra ID (Azure AD) auth is used. */
  public boolean usesConnectionString() {
    return StringUtils.isNotBlank(connectionString);
  }

  /** True when explicit service-principal credentials are supplied for Entra ID auth. */
  public boolean usesServicePrincipal() {
    return StringUtils.isNotBlank(tenantId)
        && StringUtils.isNotBlank(clientId)
        && StringUtils.isNotBlank(clientSecret);
  }
}
