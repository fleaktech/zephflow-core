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
package io.fleak.zephflow.lib.commands.kinesis;

import static io.fleak.zephflow.lib.commands.kinesis.KinesisSinkConfigValidatorTest.KINESIS_SINK_TEST_CONFIG;
import static io.fleak.zephflow.lib.commands.kinesis.KinesisSinkConfigValidatorTest.KINESIS_SINK_TEST_JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/** Created by bolei on 9/5/24 */
class KinesisSinkPartsFactoryTest {
  @Test
  public void createFlusher() {
    AwsClientFactory awsClientFactory = mock();
    KinesisClient kinesisClient = mock();
    when(awsClientFactory.createKinesisClient(any(), any()))
        .then(
            i -> {
              String regionStr = i.getArgument(0);
              UsernamePasswordCredential usernamePasswordCredential = i.getArgument(1);
              assertEquals("us-west-2", regionStr);
              assertEquals(
                  new UsernamePasswordCredential("test-access-key", "test-secret-key"),
                  usernamePasswordCredential);
              return kinesisClient;
            });
    KinesisSinkPartsFactory partsFactory =
        new KinesisSinkPartsFactory(
            null, KINESIS_SINK_TEST_JOB_CONTEXT, KINESIS_SINK_TEST_CONFIG, awsClientFactory);
    KinesisFlusher flusher = (KinesisFlusher) partsFactory.createFlusher();
    assertSame(kinesisClient, flusher.kinesisClient);
    assertEquals(KINESIS_SINK_TEST_CONFIG.getStreamName(), flusher.streamName);
    assertEquals(
        PathExpression.fromString(KINESIS_SINK_TEST_CONFIG.getPartitionKeyFieldExpressionStr()),
        flusher.partitionKeyPathExpression);
  }
}
