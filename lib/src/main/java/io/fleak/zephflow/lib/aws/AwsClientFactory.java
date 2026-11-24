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
package io.fleak.zephflow.lib.aws;

import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.TrustManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/** Created by bolei on 8/6/24 */
@Slf4j
public class AwsClientFactory implements Serializable {

  public static CloudWatchAsyncClient getCloudWatchAsyncClient(
      @Nonnull Region region,
      @Nullable URI endpoint,
      @Nullable AwsCredentialsProvider credentialsProvider) {
    var cloudWatchClientBuilder = CloudWatchAsyncClient.builder().region(region);
    if (endpoint != null) {
      log.info("Using CloudWatch endpoint {}", endpoint);
      cloudWatchClientBuilder = cloudWatchClientBuilder.endpointOverride(endpoint);
    }
    if (credentialsProvider != null) {
      cloudWatchClientBuilder = cloudWatchClientBuilder.credentialsProvider(credentialsProvider);
    }
    return cloudWatchClientBuilder.build();
  }

  public static DynamoDbAsyncClient getDynamoDbAsyncClient(
      @Nonnull Region region,
      @Nullable URI endpoint,
      @Nullable AwsCredentialsProvider credentialsProvider) {
    var dynamoClientBuilder = DynamoDbAsyncClient.builder().region(region);
    if (endpoint != null) {
      log.info("Using DynamoDB endpoint {}", endpoint);
      dynamoClientBuilder = dynamoClientBuilder.endpointOverride(endpoint);
    }
    if (credentialsProvider != null) {
      dynamoClientBuilder = dynamoClientBuilder.credentialsProvider(credentialsProvider);
    }

    return dynamoClientBuilder.build();
  }

  public static KinesisAsyncClient getKinesisAsyncClient(
      Region region, @Nullable URI endpoint, @Nullable AwsCredentialsProvider credentialsProvider) {
    var kinesisClientBuilder = KinesisAsyncClient.builder();
    if (endpoint != null) {
      log.info("Using Kinesis endpoint {}", endpoint);
      kinesisClientBuilder = kinesisClientBuilder.endpointOverride(endpoint);
    }
    if (credentialsProvider != null) {
      kinesisClientBuilder = kinesisClientBuilder.credentialsProvider(credentialsProvider);
    }

    return kinesisClientBuilder.region(region).build();
  }

  public KinesisClient createKinesisClient(
      String regionStr, UsernamePasswordCredential usernamePasswordCredential) {
    var builder = KinesisClient.builder();
    builder = setupClientBuilder(builder, regionStr, usernamePasswordCredential);
    return builder.build();
  }

  public SqsClient createSqsClient(
      String regionStr, UsernamePasswordCredential usernamePasswordCredential) {
    var builder = SqsClient.builder();
    builder = setupClientBuilder(builder, regionStr, usernamePasswordCredential);
    return builder.build();
  }

  public S3Client createS3Client(
      String regionStr,
      UsernamePasswordCredential usernamePasswordCredential,
      String s3EndpointOverride) {
    return createS3Client(regionStr, usernamePasswordCredential, s3EndpointOverride, null);
  }

  /**
   * Like {@link #createS3Client(String, UsernamePasswordCredential, String)}, and the client also
   * trusts the authorities in {@code additionalCaCertificates} (PEM) besides the JVM's default
   * ones. Blank keeps the default trust.
   */
  public S3Client createS3Client(
      String regionStr,
      UsernamePasswordCredential usernamePasswordCredential,
      String s3EndpointOverride,
      @Nullable String additionalCaCertificates) {
    S3ClientBuilder builder =
        s3ClientBuilder(regionStr, usernamePasswordCredential, s3EndpointOverride);
    if (StringUtils.isNotBlank(additionalCaCertificates)) {
      builder.httpClientBuilder(trustingAlso(ApacheHttpClient.builder(), additionalCaCertificates));
    }
    return builder.build();
  }

  public record S3ClientTimeouts(Duration connect, Duration socketRead) {}

  /**
   * A client for an S3-compatible store at {@code s3EndpointOverride}. Checksums are sent and
   * validated only when an operation requires them, because the defaults of AWS SDK 2.30+ are
   * rejected by some S3-compatible servers. The SDK does not retry: the caller owns the one retry
   * loop and the time each attempt may take.
   */
  public S3Client createS3CompatibleStorageClient(
      String regionStr,
      UsernamePasswordCredential usernamePasswordCredential,
      @Nonnull String s3EndpointOverride,
      S3ClientTimeouts timeouts) {
    return createS3CompatibleStorageClient(
        regionStr, usernamePasswordCredential, s3EndpointOverride, timeouts, null);
  }

  /**
   * Like {@link #createS3CompatibleStorageClient(String, UsernamePasswordCredential, String,
   * S3ClientTimeouts)}, and the client also trusts the authorities in {@code
   * additionalCaCertificates} (PEM) besides the JVM's default ones. Blank keeps the default trust.
   */
  public S3Client createS3CompatibleStorageClient(
      String regionStr,
      UsernamePasswordCredential usernamePasswordCredential,
      @Nonnull String s3EndpointOverride,
      S3ClientTimeouts timeouts,
      @Nullable String additionalCaCertificates) {
    ApacheHttpClient.Builder httpClient =
        ApacheHttpClient.builder()
            .connectionTimeout(timeouts.connect())
            .socketTimeout(timeouts.socketRead());
    if (StringUtils.isNotBlank(additionalCaCertificates)) {
      trustingAlso(httpClient, additionalCaCertificates);
    }
    return s3ClientBuilder(regionStr, usernamePasswordCredential, s3EndpointOverride)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
        .httpClientBuilder(httpClient)
        .overrideConfiguration(override -> override.retryStrategy(retry -> retry.maxAttempts(1)))
        .build();
  }

  private static ApacheHttpClient.Builder trustingAlso(
      ApacheHttpClient.Builder httpClient, String additionalCaCertificates) {
    TrustManager[] trustManagers =
        AdditionalCertificateAuthorities.trustManagersWithDefaultsAnd(additionalCaCertificates);
    return httpClient.tlsTrustManagersProvider(() -> trustManagers);
  }

  private S3ClientBuilder s3ClientBuilder(
      String regionStr,
      UsernamePasswordCredential usernamePasswordCredential,
      String s3EndpointOverride) {
    var builder = S3Client.builder();

    builder = setupClientBuilder(builder, regionStr, usernamePasswordCredential);

    var useForceStyle = Optional.ofNullable(System.getenv("AWS_FORCE_PATH_STYLE")).orElse("");
    if (StringUtils.isEmpty(s3EndpointOverride)) {
      s3EndpointOverride = System.getenv("AWS_ENDPOINT_URL_S3");
    }

    if (!StringUtils.isEmpty(s3EndpointOverride)) {
      log.info("Setting s3 endpoint to {}", loggableEndpoint(s3EndpointOverride));
      builder.endpointOverride(URI.create(s3EndpointOverride));
    }

    if (StringUtils.contains(s3EndpointOverride, "minio")
        || !StringUtils.equalsAnyIgnoreCase(useForceStyle, "1", "true", "enabled", "ok", "yes")) {
      log.info("Configuring s3 to use Path-Style URLs: {}", loggableEndpoint(s3EndpointOverride));
      builder.forcePathStyle(true);
    }

    return builder;
  }

  /** Scheme, host and port only: user info, path and query stay out of logs. */
  static String loggableEndpoint(String endpoint) {
    if (StringUtils.isBlank(endpoint)) {
      return "none";
    }
    URI uri = URI.create(endpoint);
    if (uri.getHost() == null) {
      return "an endpoint without a host";
    }
    return uri.getScheme()
        + "://"
        + uri.getHost()
        + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
  }

  public record S3TransferResources(S3AsyncClient asyncClient, S3TransferManager transferManager)
      implements AutoCloseable {
    @Override
    public void close() {
      transferManager.close();
      asyncClient.close();
    }
  }

  public S3TransferResources createS3TransferResources(
      String regionStr,
      UsernamePasswordCredential usernamePasswordCredential,
      String s3EndpointOverride) {

    Region region = AwsUtils.parseRegion(regionStr);
    var asyncBuilder = S3AsyncClient.builder().region(region).multipartEnabled(true);

    if (usernamePasswordCredential != null) {
      var awsCredentials =
          AwsBasicCredentials.create(
              usernamePasswordCredential.getUsername(), usernamePasswordCredential.getPassword());
      asyncBuilder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
    }

    var useForceStyle = Optional.ofNullable(System.getenv("AWS_FORCE_PATH_STYLE")).orElse("");
    if (StringUtils.isEmpty(s3EndpointOverride)) {
      s3EndpointOverride = System.getenv("AWS_ENDPOINT_URL_S3");
    }

    if (!StringUtils.isEmpty(s3EndpointOverride)) {
      log.info("Setting s3 async endpoint to {}", loggableEndpoint(s3EndpointOverride));
      asyncBuilder.endpointOverride(URI.create(s3EndpointOverride));
    }

    if (StringUtils.contains(s3EndpointOverride, "minio")
        || !StringUtils.equalsAnyIgnoreCase(useForceStyle, "1", "true", "enabled", "ok", "yes")) {
      log.info("Configuring s3 async to use Path-Style URLs");
      asyncBuilder.forcePathStyle(true);
    }

    S3AsyncClient asyncClient = asyncBuilder.build();
    S3TransferManager transferManager = S3TransferManager.builder().s3Client(asyncClient).build();
    return new S3TransferResources(asyncClient, transferManager);
  }

  private static <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT>
      BuilderT setupClientBuilder(
          BuilderT builder,
          String regionStr,
          UsernamePasswordCredential usernamePasswordCredential) {
    Region region = AwsUtils.parseRegion(regionStr);
    builder.region(region);
    // by default the SDK uses the default credential provider chain
    // see https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html
    if (usernamePasswordCredential != null) {
      var awsCredentials =
          AwsBasicCredentials.create(
              usernamePasswordCredential.getUsername(), usernamePasswordCredential.getPassword());
      builder = builder.credentialsProvider(StaticCredentialsProvider.create(awsCredentials));
    }
    return builder;
  }
}
