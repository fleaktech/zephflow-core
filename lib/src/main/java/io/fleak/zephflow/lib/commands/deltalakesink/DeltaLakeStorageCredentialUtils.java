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

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

/** Created by bolei on 9/4/25 */
public interface DeltaLakeStorageCredentialUtils {

  Map<StorageType, DeltaLakeCredentialApplier> CREDENTIAL_APPLIER_MAP =
      Map.of(
          StorageType.S3,
          new S3CredentialApplier(),
          StorageType.GCS,
          new GcsCredentialApplier(),
          StorageType.ABS,
          new AbsCredentialApplier(),
          StorageType.HDFS,
          new HdfsCredentialApplier());

  static void applyCredentials(
      StorageType storageType,
      Configuration hadoopConf,
      String tablePath,
      Object credentialObj,
      String credentialId) {
    if (storageType == StorageType.UNKNOWN) {
      return;
    }
    DeltaLakeCredentialApplier credentialApplier = CREDENTIAL_APPLIER_MAP.get(storageType);
    credentialApplier.applyCredentials(hadoopConf, tablePath, credentialObj, credentialId);
  }

  static StorageType resolveStorageType(String tablePath) {
    if (tablePath.startsWith("s3a://") || tablePath.startsWith("s3://")) {
      return StorageType.S3;
    }
    if (tablePath.startsWith("gs://")) {
      return StorageType.GCS;
    }
    if (tablePath.startsWith("abfs://") || tablePath.startsWith("abfss://")) {
      return StorageType.ABS;
    }
    if (tablePath.startsWith("hdfs://")) {
      return StorageType.HDFS;
    }
    return StorageType.UNKNOWN;
  }

  enum StorageType {
    UNKNOWN,
    S3,
    GCS,
    ABS,
    HDFS
  }

  interface DeltaLakeCredentialApplier {
    void applyCredentials(
        Configuration hadoopConf, String tablePath, Object credentialObj, String credentialId);
  }

  @Slf4j
  class S3CredentialApplier implements DeltaLakeCredentialApplier {

    @Override
    public void applyCredentials(
        Configuration hadoopConf, String tablePath, Object credentialObj, String credentialId) {
      UsernamePasswordCredential credential = (UsernamePasswordCredential) credentialObj;
      hadoopConf.set("fs.s3a.access.key", credential.getUsername());
      hadoopConf.set("fs.s3a.secret.key", credential.getPassword());
      log.info("Applied S3 credentials from credentialId: {}", credentialId);
    }
  }

  @Slf4j
  class GcsCredentialApplier implements DeltaLakeCredentialApplier {

    @Override
    public void applyCredentials(
        Configuration hadoopConf, String tablePath, Object credentialObj, String credentialId) {
      GcpCredential credential = (GcpCredential) credentialObj;
      // Set project ID
      hadoopConf.set("fs.gs.project.id", credential.getProjectId());

      switch (credential.getAuthType()) {
        case SERVICE_ACCOUNT_JSON_KEYFILE -> {
          if (credential.getJsonKeyContent() == null) {
            log.warn(
                "SERVICE_ACCOUNT_JSON_KEYFILE auth type specified but no jsonKeyContent provided");
            return;
          }

          // Parse JSON and use inline properties (no disk write for security)
          try {
            Map<String, Object> keyData =
                JsonUtils.fromJsonString(
                    credential.getJsonKeyContent(), new TypeReference<Map<String, Object>>() {});

            // Enable service account auth explicitly (overrides environment defaults)
            hadoopConf.set("fs.gs.auth.service.account.enable", "true");

            // Set inline credentials - DO NOT set google.cloud.auth.type to avoid conflict
            hadoopConf.set(
                "fs.gs.auth.service.account.email", (String) keyData.get("client_email"));
            hadoopConf.set(
                "fs.gs.auth.service.account.private.key", (String) keyData.get("private_key"));
            hadoopConf.set(
                "fs.gs.auth.service.account.private.key.id",
                (String) keyData.get("private_key_id"));

            log.info(
                "Applied GCS service account authentication via inline config from credentialId: {}",
                credentialId);
          } catch (Exception e) {
            throw new RuntimeException("Failed to parse GCP service account JSON", e);
          }
        }
        case ACCESS_TOKEN -> {
          // Use OAuth access token
          hadoopConf.set("google.cloud.auth.type", "ACCESS_TOKEN_PROVIDER");

          if (credential.getAccessToken() == null) {
            log.warn("ACCESS_TOKEN auth type specified but no accessToken provided");
            return;
          }
          // Set access token - this might require custom token provider implementation
          hadoopConf.set("google.cloud.auth.access.token", credential.getAccessToken());
          log.debug("Applied GCS access token authentication");
        }
      }
    }
  }

  @Slf4j
  class AbsCredentialApplier implements DeltaLakeCredentialApplier {
    @Override
    public void applyCredentials(
        Configuration hadoopConf, String tablePath, Object credentialObj, String credentialId) {
      ApiKeyCredential credential = (ApiKeyCredential) credentialObj;
      String storageAccount = extractAzureStorageAccount(tablePath);
      if (storageAccount == null) {
        log.warn("Could not extract storage account from Azure path: {}", tablePath);
        return;
      }
      hadoopConf.set(
          "fs.azure.account.key." + storageAccount + ".dfs.core.windows.net", credential.getKey());
      log.info(
          "Applied Azure storage account key from credentialId: {} for account: {}",
          credentialId,
          storageAccount);
    }

    String extractAzureStorageAccount(String tablePath) {
      try {
        // Extract from abfs://container@account.dfs.core.windows.net/path
        if (tablePath.contains("@")) {
          String afterAt = tablePath.split("@")[1];
          if (afterAt.contains(".")) {
            return afterAt.split("\\.")[0]; // Return just the account name
          }
        }
      } catch (Exception e) {
        log.warn("Could not extract Azure storage account from path: {}", tablePath);
      }
      return null;
    }
  }

  @Slf4j
  class HdfsCredentialApplier implements DeltaLakeCredentialApplier {
    @Override
    public void applyCredentials(
        Configuration hadoopConf, String tablePath, Object credentialObj, String credentialId) {
      log.info(
          "For HDFS authentication, configure directly via hadoopConfiguration in sink config");
    }
  }
}
