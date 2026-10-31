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

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

class KinesisSinkCommandTest {

  @Test
  void encodingSendsEachRecordAsOneEntryInThatFormat() {
    KinesisClient kinesisClient = mock();
    when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(
            PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(
                    PutRecordsResultEntry.builder().build(),
                    PutRecordsResultEntry.builder().build())
                .build());
    AwsClientFactory awsClientFactory = mock();
    when(awsClientFactory.createKinesisClient(any(), any())).thenReturn(kinesisClient);

    var command =
        new KinesisSinkCommand(
            "node",
            JOB_CONTEXT,
            new JsonConfigParser<>(KinesisSinkDto.Config.class),
            new KinesisSinkConfigValidator(),
            awsClientFactory);
    command.parseAndValidateArg(
        Map.of(
            "regionStr",
            "us-west-2",
            "streamName",
            "test-stream",
            "encoding",
            Map.of("format", Map.of("type", "json", "mode", "lines"))));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    command.writeToSink(
        List.of(
            (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "alice")),
            (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "bob"))),
        "test_user",
        command.getExecutionContext());

    ArgumentCaptor<PutRecordsRequest> sent = ArgumentCaptor.forClass(PutRecordsRequest.class);
    verify(kinesisClient).putRecords(sent.capture());
    assertEquals(
        List.of("{\"name\":\"alice\",\"id\":1}", "{\"name\":\"bob\",\"id\":2}"),
        sent.getValue().records().stream()
            .map(PutRecordsRequestEntry::data)
            .map(SdkBytes::asUtf8String)
            .toList());
  }
}
