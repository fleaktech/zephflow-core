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
package io.fleak.zephflow.lib.commands.azureeventhubsink;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubClientFactory;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AzureEventHubSinkCommandTest {

  @Test
  void encodingSendsEachRecordAsOneEventInThatFormat() {
    EventDataBatch batch = mock();
    when(batch.tryAdd(any())).thenReturn(true);
    when(batch.getCount()).thenReturn(2);
    EventHubProducerClient producerClient = mock();
    when(producerClient.createBatch()).thenReturn(batch);
    AzureEventHubClientFactory clientFactory = mock();
    when(clientFactory.createProducerClient(any())).thenReturn(producerClient);

    var command =
        (AzureEventHubSinkCommand)
            new AzureEventHubSinkCommandFactory(clientFactory).createCommand("node", JOB_CONTEXT);
    command.parseAndValidateArg(
        Map.of(
            "connectionString",
            "Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v",
            "eventHubName",
            "hub",
            "encoding",
            Map.of("format", Map.of("type", "json", "mode", "lines"))));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    command.writeToSink(
        List.of(
            (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "alice")),
            (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "bob"))),
        "test_user",
        command.getExecutionContext());

    ArgumentCaptor<EventData> sent = ArgumentCaptor.forClass(EventData.class);
    verify(batch, times(2)).tryAdd(sent.capture());
    assertEquals(
        List.of("{\"name\":\"alice\",\"id\":1}", "{\"name\":\"bob\",\"id\":2}"),
        sent.getAllValues().stream().map(EventData::getBodyAsString).toList());
  }
}
