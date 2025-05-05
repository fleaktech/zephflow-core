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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.serdes.EncodingType.CSV;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

/** Created by bolei on 9/6/24 */
class S3SinkPartsFactoryTest {

  static final S3SinkDto.Config S3SINK_TEST_CONFIG =
      S3SinkDto.Config.builder()
          .credentialId("credential_2")
          .regionStr("us-east-1")
          .bucketName("fleakdev")
          .keyName("rr-test")
          .encodingType("CSV")
          .build();

  @Test
  void createFlusher() {
    AwsClientFactory awsClientFactory = mock();
    S3Client s3Client = mock();

    when(awsClientFactory.createS3Client(any(), any(), any()))
        .then(
            i -> {
              String regionStr = i.getArgument(0);
              UsernamePasswordCredential usernamePasswordCredential = i.getArgument(1);
              assertEquals("us-east-1", regionStr);
              assertEquals(
                  new UsernamePasswordCredential("MY_USER_NAME", "MY_PASSWORD"),
                  usernamePasswordCredential);
              return s3Client;
            });

    S3SinkPartsFactory partsFactory =
        new S3SinkPartsFactory(
            new MetricClientProvider.NoopMetricClientProvider(),
            JOB_CONTEXT,
            S3SINK_TEST_CONFIG,
            awsClientFactory);
    try (S3Flusher s3Flusher = (S3Flusher) partsFactory.createFlusher()) {
      assertSame(s3Client, s3Flusher.s3Commiter.s3Client);
      assertEquals(S3SINK_TEST_CONFIG.getBucketName(), s3Flusher.s3Commiter.bucketName);
      assertInstanceOf(OnDemandS3Commiter.class, s3Flusher.s3Commiter);
      OnDemandS3Commiter onDemandS3Commiter = (OnDemandS3Commiter) s3Flusher.s3Commiter;
      assertEquals(S3SINK_TEST_CONFIG.getKeyName(), onDemandS3Commiter.keyName);
      assertEquals(CSV, onDemandS3Commiter.fleakSerializer.getEncodingType());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
