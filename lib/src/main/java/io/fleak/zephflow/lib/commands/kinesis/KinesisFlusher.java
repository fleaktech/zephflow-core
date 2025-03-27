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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

public class KinesisFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  final KinesisClient kinesisClient;
  final String streamName;
  final PathExpression partitionKeyPathExpression;
  final FleakSerializer<?> fleakSerializer;

  public KinesisFlusher(
      KinesisClient kinesisClient,
      String streamName,
      PathExpression partitionKeyPathExpression,
      FleakSerializer<?> fleakSerializer) {
    this.kinesisClient = kinesisClient;
    this.streamName = streamName;
    this.partitionKeyPathExpression = partitionKeyPathExpression;
    this.fleakSerializer = fleakSerializer;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents) {
    List<PutRecordsRequestEntry> records = new ArrayList<>();
    List<ErrorOutput> errorOutputs = new ArrayList<>();

    for (Pair<RecordFleakData, RecordFleakData> pair : preparedInputEvents.rawAndPreparedList()) {
      try {
        SerializedEvent serializedEvent = fleakSerializer.serialize(List.of(pair.getRight()));
        if (serializedEvent.value() == null) {
          throw new IllegalArgumentException(
              String.format("JSON serialization resulted in null for record %s", pair.getRight()));
        }
        String partitionKey =
            partitionKeyPathExpression.getStringValueFromEventOrDefault(
                pair.getRight(), UUID.randomUUID().toString());

        PutRecordsRequestEntry entry =
            PutRecordsRequestEntry.builder()
                .partitionKey(partitionKey)
                .data(SdkBytes.fromByteArray(serializedEvent.value()))
                .build();
        records.add(entry);
      } catch (Exception e) {
        errorOutputs.add(
            new ErrorOutput(pair.getLeft(), "Failed to process record: " + e.getMessage()));
      }
    }

    if (records.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, errorOutputs);
    }

    PutRecordsRequest putRecordsRequest =
        PutRecordsRequest.builder().streamName(streamName).records(records).build();

    try {
      PutRecordsResponse putRecordsResponse = kinesisClient.putRecords(putRecordsRequest);

      if (putRecordsResponse == null) {
        throw new IllegalStateException("Received null response from Kinesis client");
      }

      int successCount =
          putRecordsResponse.records().size() - putRecordsResponse.failedRecordCount();

      for (int i = 0; i < putRecordsResponse.records().size(); i++) {
        if (putRecordsResponse.records().get(i).errorCode() != null) {
          ErrorOutput errorOutput =
              new ErrorOutput(
                  preparedInputEvents.rawAndPreparedList().get(i).getLeft(),
                  putRecordsResponse.records().get(i).errorMessage());
          errorOutputs.add(errorOutput);
        }
      }

      return new SimpleSinkCommand.FlushResult(successCount, errorOutputs);
    } catch (Exception e) {
      // Handle any exceptions from the Kinesis client, including null response
      for (Pair<RecordFleakData, RecordFleakData> pair : preparedInputEvents.rawAndPreparedList()) {
        errorOutputs.add(
            new ErrorOutput(pair.getLeft(), "Kinesis client error: " + e.getMessage()));
      }
      return new SimpleSinkCommand.FlushResult(0, errorOutputs);
    }
  }

  @Override
  public void close() {
    kinesisClient.close();
  }
}
