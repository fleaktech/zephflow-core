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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeStorageCredentialUtils.CREDENTIAL_APPLIER_MAP;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeltaLakeStorageCredentialUtilsTest {

  private Configuration hadoopConf;

  @BeforeEach
  void setUp() {
    hadoopConf = new Configuration(false); // Create empty configuration without defaults
  }

  @Test
  void testCredentialApplierMapContainsAllStorageTypes() {
    Map<
            DeltaLakeStorageCredentialUtils.StorageType,
            DeltaLakeStorageCredentialUtils.DeltaLakeCredentialApplier>
        applierMap = CREDENTIAL_APPLIER_MAP;

    assertEquals(4, applierMap.size());
    assertTrue(applierMap.containsKey(DeltaLakeStorageCredentialUtils.StorageType.S3));
    assertTrue(applierMap.containsKey(DeltaLakeStorageCredentialUtils.StorageType.GCS));
    assertTrue(applierMap.containsKey(DeltaLakeStorageCredentialUtils.StorageType.ABS));
    assertTrue(applierMap.containsKey(DeltaLakeStorageCredentialUtils.StorageType.HDFS));
  }

  @Test
  void testResolveStorageType() {
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.S3,
        DeltaLakeStorageCredentialUtils.resolveStorageType("s3a://bucket/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.S3,
        DeltaLakeStorageCredentialUtils.resolveStorageType("s3://bucket/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.GCS,
        DeltaLakeStorageCredentialUtils.resolveStorageType("gs://bucket/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.ABS,
        DeltaLakeStorageCredentialUtils.resolveStorageType(
            "abfs://container@account.dfs.core.windows.net/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.ABS,
        DeltaLakeStorageCredentialUtils.resolveStorageType(
            "abfss://container@account.dfs.core.windows.net/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.HDFS,
        DeltaLakeStorageCredentialUtils.resolveStorageType("hdfs://namenode:9000/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.UNKNOWN,
        DeltaLakeStorageCredentialUtils.resolveStorageType("file:///local/path"));
    assertEquals(
        DeltaLakeStorageCredentialUtils.StorageType.UNKNOWN,
        DeltaLakeStorageCredentialUtils.resolveStorageType("unknown://path"));
  }

  @Test
  void testApplyCredentialsWithUnknownStorageType() {
    assertDoesNotThrow(
        () ->
            DeltaLakeStorageCredentialUtils.applyCredentials(
                DeltaLakeStorageCredentialUtils.StorageType.UNKNOWN,
                hadoopConf,
                "file:///path",
                null,
                "cred-id"));
  }

  @Test
  void testApplyCredentialsWithValidStorageType() {
    UsernamePasswordCredential credential =
        new UsernamePasswordCredential("access-key", "secret-key");

    assertDoesNotThrow(
        () ->
            DeltaLakeStorageCredentialUtils.applyCredentials(
                DeltaLakeStorageCredentialUtils.StorageType.S3,
                hadoopConf,
                "s3a://bucket/path",
                credential,
                "s3-cred"));

    assertEquals("access-key", hadoopConf.get("fs.s3a.access.key"));
    assertEquals("secret-key", hadoopConf.get("fs.s3a.secret.key"));
  }

  @Test
  void testS3CredentialApplier() {
    DeltaLakeStorageCredentialUtils.S3CredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.S3CredentialApplier();
    UsernamePasswordCredential credential =
        new UsernamePasswordCredential("access-key", "secret-key");

    applier.applyCredentials(hadoopConf, "s3a://bucket/path", credential, "s3-cred");

    assertEquals("access-key", hadoopConf.get("fs.s3a.access.key"));
    assertEquals("secret-key", hadoopConf.get("fs.s3a.secret.key"));
  }

  @Test
  void testGcsCredentialApplierWithServiceAccountJson() {
    DeltaLakeStorageCredentialUtils.GcsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.GcsCredentialApplier();
    // Use proper GCP service account JSON format with required fields
    String jsonContent =
        """
        {
          "type": "service_account",
          "project_id": "test-project",
          "private_key_id": "key123",
          "private_key": "-----BEGIN PRIVATE KEY-----\\nMIItest\\n-----END PRIVATE KEY-----\\n",
          "client_email": "test@test-project.iam.gserviceaccount.com"
        }
        """;
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .jsonKeyContent(jsonContent)
            .projectId("test-project")
            .build();

    applier.applyCredentials(hadoopConf, "gs://bucket/path", credential, "gcs-cred");

    // Verify inline properties are set (no temp file)
    assertEquals("test-project", hadoopConf.get("fs.gs.project.id"));
    assertEquals("true", hadoopConf.get("fs.gs.auth.service.account.enable"));
    assertEquals(
        "test@test-project.iam.gserviceaccount.com",
        hadoopConf.get("fs.gs.auth.service.account.email"));
    assertEquals("key123", hadoopConf.get("fs.gs.auth.service.account.private.key.id"));
    assertNotNull(hadoopConf.get("fs.gs.auth.service.account.private.key"));

    // Verify no file-based auth type is set (would conflict with inline)
    assertNull(hadoopConf.get("google.cloud.auth.type"));
    assertNull(hadoopConf.get("google.cloud.auth.service.account.json.keyfile"));
  }

  @Test
  void testGcsCredentialApplierWithAccessToken() {
    DeltaLakeStorageCredentialUtils.GcsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.GcsCredentialApplier();
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.ACCESS_TOKEN)
            .accessToken("ya29.access-token")
            .projectId("test-project")
            .build();

    applier.applyCredentials(hadoopConf, "gs://bucket/path", credential, "gcs-cred");

    assertEquals("test-project", hadoopConf.get("fs.gs.project.id"));
    assertEquals("ACCESS_TOKEN_PROVIDER", hadoopConf.get("google.cloud.auth.type"));
    assertEquals("ya29.access-token", hadoopConf.get("google.cloud.auth.access.token"));
  }

  @Test
  void testGcsCredentialApplierWithMissingJsonContent() {
    DeltaLakeStorageCredentialUtils.GcsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.GcsCredentialApplier();
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .projectId("test-project")
            .build();

    applier.applyCredentials(hadoopConf, "gs://bucket/path", credential, "gcs-cred");

    assertEquals("test-project", hadoopConf.get("fs.gs.project.id"));
    assertNull(hadoopConf.get("google.cloud.auth.service.account.json.keyfile"));
  }

  @Test
  void testGcsCredentialApplierWithMissingAccessToken() {
    DeltaLakeStorageCredentialUtils.GcsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.GcsCredentialApplier();
    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.ACCESS_TOKEN)
            .projectId("test-project")
            .build();

    applier.applyCredentials(hadoopConf, "gs://bucket/path", credential, "gcs-cred");

    assertEquals("test-project", hadoopConf.get("fs.gs.project.id"));
    assertEquals("ACCESS_TOKEN_PROVIDER", hadoopConf.get("google.cloud.auth.type"));
    assertNull(hadoopConf.get("google.cloud.auth.access.token"));
  }

  @Test
  void testAbsCredentialApplier() {
    DeltaLakeStorageCredentialUtils.AbsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.AbsCredentialApplier();
    ApiKeyCredential credential = new ApiKeyCredential("storage-account-key");

    applier.applyCredentials(
        hadoopConf,
        "abfs://container@storageaccount.dfs.core.windows.net/path",
        credential,
        "abs-cred");

    assertEquals(
        "storage-account-key",
        hadoopConf.get("fs.azure.account.key.storageaccount.dfs.core.windows.net"));
  }

  @Test
  void testAbsCredentialApplierWithAbfss() {
    DeltaLakeStorageCredentialUtils.AbsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.AbsCredentialApplier();
    ApiKeyCredential credential = new ApiKeyCredential("storage-account-key");

    applier.applyCredentials(
        hadoopConf,
        "abfss://container@mystorageaccount.dfs.core.windows.net/path",
        credential,
        "abs-cred");

    assertEquals(
        "storage-account-key",
        hadoopConf.get("fs.azure.account.key.mystorageaccount.dfs.core.windows.net"));
  }

  @Test
  void testAbsCredentialApplierWithInvalidPath() {
    DeltaLakeStorageCredentialUtils.AbsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.AbsCredentialApplier();
    ApiKeyCredential credential = new ApiKeyCredential("storage-account-key");

    applier.applyCredentials(hadoopConf, "abfs://invalid-path", credential, "abs-cred");

    // When path is invalid, no Azure storage account key should be set
    assertFalse(hadoopConf.iterator().hasNext());
  }

  @Test
  void testHdfsCredentialApplier() {
    DeltaLakeStorageCredentialUtils.HdfsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.HdfsCredentialApplier();

    assertDoesNotThrow(
        () -> applier.applyCredentials(hadoopConf, "hdfs://namenode:9000/path", null, "hdfs-cred"));

    // HDFS applier doesn't set any configuration properties
    assertFalse(hadoopConf.iterator().hasNext());
  }

  @Test
  void testGcsCredentialApplierWithInvalidJson() {
    DeltaLakeStorageCredentialUtils.GcsCredentialApplier applier =
        new DeltaLakeStorageCredentialUtils.GcsCredentialApplier();

    GcpCredential credential =
        GcpCredential.builder()
            .authType(GcpCredential.AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .jsonKeyContent("not valid json {{{")
            .projectId("test-project")
            .build();

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> applier.applyCredentials(hadoopConf, "gs://bucket/path", credential, "gcs-cred"));

    assertEquals("Failed to parse GCP service account JSON", exception.getMessage());
  }
}
