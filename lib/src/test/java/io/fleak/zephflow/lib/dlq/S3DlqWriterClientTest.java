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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.invocation.Invocation;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;

class S3DlqWriterClientTest {

  private static final String ENDPOINT = "http://storage.cluster.local:8333";

  record ClientSettings(
      Optional<URI> endpointOverride,
      RequestChecksumCalculation requestChecksumCalculation,
      ResponseChecksumValidation responseChecksumValidation,
      Optional<Duration> apiCallAttemptTimeout,
      Optional<Duration> apiCallTimeout,
      Optional<Integer> retryMaxAttempts,
      List<Duration> connectionTimeouts,
      List<Duration> socketTimeouts) {}

  @Test
  void clientForAnEndpointOverrideSendsChecksumsOnlyWhenRequiredAndTimesOut() throws IOException {
    assertEquals(
        new ClientSettings(
            Optional.of(URI.create(ENDPOINT)),
            RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation.WHEN_REQUIRED,
            Optional.empty(),
            Optional.empty(),
            Optional.of(1),
            List.of(Duration.ofSeconds(5)),
            List.of(Duration.ofSeconds(30))),
        clientSettingsOfWriterFor(ENDPOINT));
  }

  @Test
  void clientForAwsWithoutEndpointOverrideIsTheFactoryDefaultClient() throws IOException {
    ClientSettings factoryDefault;
    try (S3Client client =
        new AwsClientFactory()
            .createS3Client(
                "us-east-1",
                new UsernamePasswordCredential(
                    "access-" + UUID.randomUUID(), "x-" + UUID.randomUUID()),
                null)) {
      factoryDefault = clientSettings(client, List.of());
    }

    assertEquals(factoryDefault, clientSettingsOfWriterFor(null));
  }

  private static ClientSettings clientSettingsOfWriterFor(String endpoint) throws IOException {
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("dlq")
            .batchSize(100)
            .flushIntervalMillis(10_000)
            .accessKeyId("access-" + UUID.randomUUID())
            .secretAccessKey("x-" + UUID.randomUUID())
            .s3EndpointOverride(endpoint)
            .build();
    List<ApacheHttpClient.Builder> httpClientBuilders = new ArrayList<>();
    try (MockedStatic<ApacheHttpClient> apacheHttpClient =
        mockStatic(ApacheHttpClient.class, CALLS_REAL_METHODS)) {
      apacheHttpClient
          .when(ApacheHttpClient::builder)
          .thenAnswer(
              invocation -> {
                ApacheHttpClient.Builder builder =
                    spy((ApacheHttpClient.Builder) invocation.callRealMethod());
                httpClientBuilders.add(builder);
                return builder;
              });
      try (S3DlqWriter writer =
          (S3DlqWriter)
              DlqWriterFactory.createDlqWriter(
                  config,
                  "prefix",
                  new MetricClientProvider.NoopMetricClientProvider(),
                  Map.of())) {
        return clientSettings(writer.s3Client, httpClientBuilders);
      }
    }
  }

  private static ClientSettings clientSettings(
      S3Client client, List<ApacheHttpClient.Builder> httpClientBuilders) {
    S3ServiceClientConfiguration configuration = client.serviceClientConfiguration();
    return new ClientSettings(
        configuration.endpointOverride(),
        configuration.requestChecksumCalculation(),
        configuration.responseChecksumValidation(),
        configuration.overrideConfiguration().apiCallAttemptTimeout(),
        configuration.overrideConfiguration().apiCallTimeout(),
        configuration.overrideConfiguration().retryStrategy().map(RetryStrategy::maxAttempts),
        argumentsOf(httpClientBuilders, "connectionTimeout"),
        argumentsOf(httpClientBuilders, "socketTimeout"));
  }

  private static List<Duration> argumentsOf(
      List<ApacheHttpClient.Builder> builders, String methodName) {
    return builders.stream()
        .flatMap(builder -> mockingDetails(builder).getInvocations().stream())
        .filter(invocation -> invocation.getMethod().getName().equals(methodName))
        .map(Invocation::getArguments)
        .map(arguments -> (Duration) arguments[0])
        .toList();
  }
}
