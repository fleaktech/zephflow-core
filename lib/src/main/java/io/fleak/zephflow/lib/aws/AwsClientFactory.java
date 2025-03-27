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
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3Client;

/** Created by bolei on 8/6/24 */
@Slf4j
public class AwsClientFactory implements Serializable {

  public KinesisClient createKinesisClient(
      String regionStr, UsernamePasswordCredential usernamePasswordCredential) {
    var builder = KinesisClient.builder();
    builder = setupClientBuilder(builder, regionStr, usernamePasswordCredential);
    return builder.build();
  }

  public S3Client createS3Client(
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
      log.info("Setting s3 endpoint to {}", s3EndpointOverride);
      builder.endpointOverride(URI.create(s3EndpointOverride));
    }

    if (StringUtils.contains(s3EndpointOverride, "minio")
        || !StringUtils.equalsAnyIgnoreCase(useForceStyle, "1", "true", "enabled", "ok", "yes")) {
      log.info("Configuring s3 to use Path-Style URLs");
      builder.forcePathStyle(true);
    }

    return builder.build();
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
