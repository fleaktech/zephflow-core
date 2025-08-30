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
package io.fleak.zephflow.lib.credentials;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Credential for Google Cloud Platform services (GCS, BigQuery, etc.)
 *
 * <p>Supported authentication methods:
 *
 * <ul>
 *   <li>OAuth Access Token - Recommended (keeps credentials in memory only)
 *   <li>Service Account JSON keyfile - Supported but NOT recommended (writes credentials to temp
 *       file on disk)
 * </ul>
 *
 * <p>Usage examples:
 *
 * <pre>
 * // OAuth Access Token (recommended - secure in-memory auth)
 * GcpCredential credential = GcpCredential.builder()
 *     .authType(AuthType.ACCESS_TOKEN)
 *     .accessToken("ya29.c.ElqKB...")
 *     .projectId("my-project-123")
 *     .build();
 *
 * // Service Account JSON (supported but NOT recommended - security risk)
 * GcpCredential credential = GcpCredential.builder()
 *     .authType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
 *     .jsonKeyContent("{\"type\":\"service_account\",...}")
 *     .projectId("my-project-123")
 *     .build();
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GcpCredential implements Serializable {

  /** GCP authentication types */
  public enum AuthType {
    /** Use service account JSON keyfile for authentication */
    SERVICE_ACCOUNT_JSON_KEYFILE,
    /** Use OAuth access token for authentication */
    ACCESS_TOKEN
  }

  /** Authentication type */
  @NonNull AuthType authType;

  /**
   * Full JSON content of service account keyfile when authType is SERVICE_ACCOUNT_JSON_KEYFILE.
   * Should contain the complete JSON structure with private_key, client_email, etc.
   */
  String jsonKeyContent;

  /**
   * OAuth access token when authType is ACCESS_TOKEN. Format: "ya29.c...." (temporary token that
   * expires)
   */
  String accessToken;

  /** GCP project ID associated with this credential */
  @NonNull String projectId;
}
