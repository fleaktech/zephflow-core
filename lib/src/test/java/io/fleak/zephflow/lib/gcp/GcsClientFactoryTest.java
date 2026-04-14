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
package io.fleak.zephflow.lib.gcp;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.credentials.GcpCredential;
import org.junit.jupiter.api.Test;

class GcsClientFactoryTest {

  @Test
  void testCreateStorageClientWithInvalidJson() {
    GcsClientFactory factory = new GcsClientFactory();
    assertThrows(RuntimeException.class, () -> factory.createStorageClient("not-valid-json"));
  }

  @Test
  void testCreateStorageClientWithEmptyJson() {
    GcsClientFactory factory = new GcsClientFactory();
    assertThrows(RuntimeException.class, () -> factory.createStorageClient("{}"));
  }

  @Test
  void testCreateStorageClientWithNullFallsBackToAdc() {
    GcsClientFactory factory = new GcsClientFactory();
    // In CI without GCP credentials, ADC will throw. Verify we enter the ADC path
    // by checking the exception message indicates Application Default Credentials.
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> factory.createStorageClient((String) null));
    assertTrue(
        ex.getMessage().contains("Application Default Credentials"),
        "Expected ADC error but got: " + ex.getMessage());
  }

  @Test
  void testCreateStorageClientWithBlankFallsBackToAdc() {
    GcsClientFactory factory = new GcsClientFactory();
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> factory.createStorageClient("   "));
    assertTrue(
        ex.getMessage().contains("Application Default Credentials"),
        "Expected ADC error but got: " + ex.getMessage());
  }

  @Test
  void testCreateStorageClientNoArgFallsBackToAdc() {
    GcsClientFactory factory = new GcsClientFactory();
    RuntimeException ex = assertThrows(RuntimeException.class, factory::createStorageClient);
    assertTrue(
        ex.getMessage().contains("Application Default Credentials"),
        "Expected ADC error but got: " + ex.getMessage());
  }

  @Test
  void testCreateStorageClientWithGcpCredentialAdc() {
    GcsClientFactory factory = new GcsClientFactory();
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.APPLICATION_DEFAULT)
            .projectId("test-project")
            .build();
    // ADC will throw in CI without GCP credentials — verify the ADC path is entered
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> factory.createStorageClient(credential));
    assertTrue(
        ex.getMessage().contains("Failed to create GCS storage client"),
        "Expected GCS client creation error but got: " + ex.getMessage());
  }

  @Test
  void testCreateStorageClientWithGcpCredentialAdcNullProjectId() {
    GcsClientFactory factory = new GcsClientFactory();
    GcpCredential credential =
        GcpCredential.builder().authType(GcpCredential.AuthType.APPLICATION_DEFAULT).build();
    // Verify null projectId does not cause NPE — should still reach ADC path
    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> factory.createStorageClient(credential));
    assertTrue(
        ex.getMessage().contains("Failed to create GCS storage client"),
        "Expected GCS client creation error but got: " + ex.getMessage());
  }

  @Test
  void testCreateStorageClientWithGcpCredentialAccessToken() {
    GcsClientFactory factory = new GcsClientFactory();
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.ACCESS_TOKEN)
            .accessToken("ya29.test-token")
            .projectId("test-project")
            .build();
    // Access token credentials don't need network — this should succeed
    assertDoesNotThrow(() -> factory.createStorageClient(credential));
  }

  @Test
  void testCreateStorageClientWithGcpCredentialServiceAccountJson() {
    GcsClientFactory factory = new GcsClientFactory();
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .jsonKeyContent("not-valid-json")
            .projectId("test-project")
            .build();
    assertThrows(RuntimeException.class, () -> factory.createStorageClient(credential));
  }
}
