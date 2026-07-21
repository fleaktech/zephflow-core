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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/**
 * Shared configuration checks for the Event Hub source and sink, so both enforce auth uniformly.
 */
public final class AzureEventHubConfigValidation {

  private AzureEventHubConfigValidation() {}

  /**
   * Enforces exactly one Event Hub authentication mode: either a SAS connection string, or Entra ID
   * via the fully qualified namespace. When Entra ID service-principal fields are supplied they
   * must all be present together.
   */
  public static void validateEventHubAuth(
      String connectionString,
      String fullyQualifiedNamespace,
      String tenantId,
      String clientId,
      String clientSecret) {
    boolean hasConnectionString = StringUtils.isNotBlank(connectionString);
    boolean hasNamespace = StringUtils.isNotBlank(fullyQualifiedNamespace);

    Preconditions.checkArgument(
        hasConnectionString || hasNamespace,
        "Event Hub auth requires either connectionString or fullyQualifiedNamespace");
    Preconditions.checkArgument(
        !(hasConnectionString && hasNamespace),
        "provide only one of connectionString or fullyQualifiedNamespace, not both");

    int servicePrincipalFields =
        (StringUtils.isNotBlank(tenantId) ? 1 : 0)
            + (StringUtils.isNotBlank(clientId) ? 1 : 0)
            + (StringUtils.isNotBlank(clientSecret) ? 1 : 0);
    Preconditions.checkArgument(
        servicePrincipalFields == 0 || servicePrincipalFields == 3,
        "Entra ID service principal requires all of tenantId, clientId and clientSecret");
    Preconditions.checkArgument(
        servicePrincipalFields == 0 || hasNamespace,
        "service principal credentials require fullyQualifiedNamespace (Entra ID auth)");
  }
}
