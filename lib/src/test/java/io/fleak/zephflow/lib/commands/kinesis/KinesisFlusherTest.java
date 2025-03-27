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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

class KinesisFlusherTest {

  private static final String STREAM_NAME = "test-stream";
  private KinesisClient kinesisClient;
  private KinesisFlusher flusher;

  @BeforeEach
  void setUp() {
    kinesisClient = mock(KinesisClient.class);
    PathExpression partitionKeyPathExpression = PathExpression.fromString("$.partitionKey");

    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT);
    FleakSerializer<?> serializer = serializerFactory.createSerializer();

    flusher =
        new KinesisFlusher(kinesisClient, STREAM_NAME, partitionKeyPathExpression, serializer);
  }

  @Test
  void testSuccessfulFlush() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))));
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2"))));

    PutRecordsResponse mockResponse =
        PutRecordsResponse.builder()
            .failedRecordCount(0)
            .records(
                PutRecordsResultEntry.builder().build(), PutRecordsResultEntry.builder().build())
            .build();

    when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(mockResponse);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents);

    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());

    verify(kinesisClient)
        .putRecords(
            argThat(
                (PutRecordsRequest request) -> {
                  assertEquals(STREAM_NAME, request.streamName());
                  assertEquals(2, request.records().size());
                  assertEquals("key1", request.records().get(0).partitionKey());
                  assertEquals("key2", request.records().get(1).partitionKey());
                  return true;
                }));
  }

  @Test
  void testPartialFailure() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))));
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2"))));

    PutRecordsResponse mockResponse =
        PutRecordsResponse.builder()
            .failedRecordCount(1)
            .records(
                PutRecordsResultEntry.builder().build(),
                PutRecordsResultEntry.builder()
                    .errorCode("InternalFailure")
                    .errorMessage("Internal error")
                    .build())
            .build();

    when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(mockResponse);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents);

    assertEquals(1, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertEquals("Internal error", result.errorOutputList().getFirst().errorMessage());
  }

  @Test
  void testNullResponseFromKinesisClient() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))));

    when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(null);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents);

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().getFirst().errorMessage().contains("Kinesis client error"));
    assertTrue(
        result
            .errorOutputList()
            .getFirst()
            .errorMessage()
            .contains("Received null response from Kinesis client"));

    verify(kinesisClient).putRecords(any(PutRecordsRequest.class));
  }

  @Test
  void testEmptyBatch() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents);

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(kinesisClient, never()).putRecords(any(PutRecordsRequest.class));
  }
}
