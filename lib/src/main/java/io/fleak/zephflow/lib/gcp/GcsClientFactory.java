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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class GcsClientFactory implements Serializable {

  public Storage createStorageClient() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create GCS storage client from Application Default Credentials", e);
    }
  }

  public Storage createStorageClient(String serviceAccountJson) {
    if (StringUtils.isBlank(serviceAccountJson)) {
      return createStorageClient();
    }
    try {
      ServiceAccountCredentials credentials =
          ServiceAccountCredentials.fromStream(
              new ByteArrayInputStream(serviceAccountJson.getBytes(StandardCharsets.UTF_8)));
      return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create GCS storage client from service account JSON", e);
    }
  }

  public Storage createStorageClient(GcpCredential credential) {
    try {
      GoogleCredentials googleCredentials = resolveCredentials(credential);
      StorageOptions.Builder builder =
          StorageOptions.newBuilder().setCredentials(googleCredentials);
      if (credential.getProjectId() != null) {
        builder.setProjectId(credential.getProjectId());
      }
      return builder.build().getService();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create GCS storage client", e);
    }
  }

  private GoogleCredentials resolveCredentials(GcpCredential credential) throws IOException {
    return switch (credential.getAuthType()) {
      case SERVICE_ACCOUNT_JSON_KEYFILE -> {
        log.info("Creating GCS client using service account JSON keyfile");
        yield ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(
                credential.getJsonKeyContent().getBytes(StandardCharsets.UTF_8)));
      }
      case ACCESS_TOKEN -> {
        log.info("Creating GCS client using OAuth access token");
        yield GoogleCredentials.create(new AccessToken(credential.getAccessToken(), null));
      }
      case APPLICATION_DEFAULT -> {
        log.info("Creating GCS client using Application Default Credentials");
        yield GoogleCredentials.getApplicationDefault();
      }
    };
  }
}
