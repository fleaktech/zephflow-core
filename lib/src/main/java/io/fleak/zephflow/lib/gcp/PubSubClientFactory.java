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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcPublisherStub;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubClientFactory implements Serializable {

  public SubscriberStub createSubscriberStub() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      SubscriberStubSettings settings =
          SubscriberStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcSubscriberStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Pub/Sub subscriber stub from Application Default Credentials", e);
    }
  }

  public SubscriberStub createSubscriberStub(GcpCredential credential) {
    try {
      GoogleCredentials credentials = resolveCredentials(credential);
      SubscriberStubSettings settings =
          SubscriberStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcSubscriberStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Pub/Sub subscriber stub", e);
    }
  }

  public PublisherStub createPublisherStub() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      PublisherStubSettings settings =
          PublisherStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcPublisherStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Pub/Sub publisher stub from Application Default Credentials", e);
    }
  }

  public PublisherStub createPublisherStub(GcpCredential credential) {
    try {
      GoogleCredentials credentials = resolveCredentials(credential);
      PublisherStubSettings settings =
          PublisherStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcPublisherStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Pub/Sub publisher stub", e);
    }
  }

  private GoogleCredentials resolveCredentials(GcpCredential credential) throws IOException {
    return switch (credential.getAuthType()) {
      case SERVICE_ACCOUNT_JSON_KEYFILE -> {
        log.info("Creating Pub/Sub client using service account JSON keyfile");
        yield ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(
                credential.getJsonKeyContent().getBytes(StandardCharsets.UTF_8)));
      }
      case ACCESS_TOKEN -> {
        log.info("Creating Pub/Sub client using OAuth access token");
        yield GoogleCredentials.create(new AccessToken(credential.getAccessToken(), null));
      }
      case APPLICATION_DEFAULT -> {
        log.info("Creating Pub/Sub client using Application Default Credentials");
        yield GoogleCredentials.getApplicationDefault();
      }
    };
  }
}
