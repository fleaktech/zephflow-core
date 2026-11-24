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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Created by bolei on 11/13/24 */
class AwsClientFactoryTest {

  @Test
  void createS3Client_nullS3EndpointOverride() {
    try (S3Client s3Client =
        new AwsClientFactory().createS3Client(Region.US_EAST_1.toString(), null, null)) {
      // should not hit NPE
      System.out.println(s3Client);
    }
  }

  @Test
  void endpointLogsCarryNeitherUserInfoNorPathAndTheClientKeepsTheWholeEndpoint() {
    String endpoint =
        "http://storage-user:" + "x-" + System.nanoTime() + "@storage.local:8333/a?b=c";
    List<String> messages;
    Optional<URI> endpointOverride;
    try (BoundedLogCapture capture = new BoundedLogCapture(AwsClientFactory.class);
        S3Client s3Client =
            new AwsClientFactory().createS3Client(Region.US_EAST_1.toString(), null, endpoint)) {
      messages =
          capture.events().stream().map(event -> event.getMessage().getFormattedMessage()).toList();
      endpointOverride = s3Client.serviceClientConfiguration().endpointOverride();
    }

    assertEquals(
        List.of(
            "Setting s3 endpoint to http://storage.local:8333",
            "Configuring s3 to use Path-Style URLs: http://storage.local:8333"),
        messages);
    assertEquals(Optional.of(URI.create(endpoint)), endpointOverride);
  }
}
