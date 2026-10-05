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
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BoundedFlushException;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

@Slf4j
public class KinesisFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  final KinesisClient kinesisClient;
  final String streamName;
  final PathExpression partitionKeyPathExpression;
  final FleakSerializer<?> fleakSerializer;

  public KinesisFlusher(
      KinesisClient kinesisClient,
      String streamName,
      @Nullable PathExpression partitionKeyPathExpression,
      FleakSerializer<?> fleakSerializer) {
    this.kinesisClient = kinesisClient;
    this.streamName = streamName;
    this.partitionKeyPathExpression = partitionKeyPathExpression;
    this.fleakSerializer = fleakSerializer;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
      Map<String, String> metricTags) {
    log.debug(
        "Kinesis flushing started for {}, list {}",
        streamName,
        preparedInputEvents.preparedList().size());
    List<PutRecordsRequestEntry> records = new ArrayList<>();
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    List<Integer> recordSizes = new ArrayList<>();

    for (Pair<RecordFleakData, RecordFleakData> pair : preparedInputEvents.rawAndPreparedList()) {
      try {
        RecordFleakData recordFleakData = pair.getRight();
        SerializedEvent serializedEvent = fleakSerializer.serialize(List.of(recordFleakData));
        if (serializedEvent.value() == null) {
          throw new IllegalArgumentException(
              String.format("JSON serialization resulted in null for record %s", pair.getRight()));
        }
        recordSizes.add(serializedEvent.value().length);
        String partitionKey =
            partitionKeyPathExpression == null
                ? UUID.randomUUID().toString()
                : partitionKeyPathExpression.getStringValueFromEventOrDefault(
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
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
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

      long flushedDataSize = 0;
      for (int i = 0; i < putRecordsResponse.records().size(); i++) {
        if (putRecordsResponse.records().get(i).errorCode() != null) {
          ErrorOutput errorOutput =
              new ErrorOutput(
                  preparedInputEvents.rawAndPreparedList().get(i).getLeft(),
                  putRecordsResponse.records().get(i).errorMessage());
          errorOutputs.add(errorOutput);
        } else {
          int recordSize = recordSizes.get(i);
          flushedDataSize += recordSize;
        }
      }

      return new SimpleSinkCommand.FlushResult(successCount, flushedDataSize, errorOutputs);
    } catch (Exception e) {
      // Handle any exceptions from the Kinesis client, including null response
      for (Pair<RecordFleakData, RecordFleakData> pair : preparedInputEvents.rawAndPreparedList()) {
        errorOutputs.add(
            new ErrorOutput(pair.getLeft(), "Kinesis client error: " + e.getMessage()));
      }
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
    }
  }

  @Override
  public SimpleSinkCommand.FlushResult flushBounded(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events,
      Map<String, String> metricTags,
      ExecutionHooks hooks) {
    List<PutRecordsRequestEntry> entries = new ArrayList<>();
    List<RecordFleakData> submitted = new ArrayList<>();
    List<Integer> sizes = new ArrayList<>();
    List<ErrorOutput> errors = new ArrayList<>();
    for (var pair : events.rawAndPreparedList()) {
      try {
        var serialized = fleakSerializer.serialize(List.of(pair.getRight()));
        if (serialized.value() == null)
          throw new IllegalArgumentException("Serialization produced no record bytes");
        String key =
            partitionKeyPathExpression == null
                ? UUID.randomUUID().toString()
                : partitionKeyPathExpression.getStringValueFromEventOrDefault(
                    pair.getRight(), UUID.randomUUID().toString());
        var entry =
            PutRecordsRequestEntry.builder()
                .partitionKey(key)
                .data(SdkBytes.fromByteArray(serialized.value()))
                .build();
        entries.add(entry);
        submitted.add(pair.getLeft());
        sizes.add(serialized.value().length);
      } catch (Exception failure) {
        errors.add(
            new ErrorOutput(pair.getLeft(), "Failed to process record: " + failure.getMessage()));
      }
    }
    long notAttempted = events.rawAndPreparedList().size() - entries.size();
    if (entries.isEmpty()) return boundedResult(0, 0, notAttempted, 0, notAttempted, 0, errors);
    try {
      hooks.control().checkpoint();
    } catch (ExecutionStoppedException stopped) {
      throw new BoundedFlushException(
          boundedResult(0, 0, notAttempted, 0, events.rawAndPreparedList().size(), 0, errors),
          stopped);
    }
    int acknowledged = 0, rejected = 0, observed = 0;
    long bytes = 0;
    try {
      var response =
          kinesisClient.putRecords(
              PutRecordsRequest.builder().streamName(streamName).records(entries).build());
      if (response == null)
        throw new IllegalStateException("Received null response from Kinesis client");
      for (int index = 0; index < Math.min(response.records().size(), submitted.size()); index++) {
        var receipt = response.records().get(index);
        if (receipt != null && StringUtils.isNotBlank(receipt.errorCode())) {
          rejected++;
          errors.add(new ErrorOutput(submitted.get(index), receipt.errorMessage()));
        } else if (receipt != null
            && StringUtils.isNotBlank(receipt.sequenceNumber())
            && StringUtils.isNotBlank(receipt.shardId())) {
          acknowledged++;
          bytes += sizes.get(index);
        } else {
          errors.add(
              new ErrorOutput(
                  submitted.get(index), "Kinesis delivery acknowledgement unavailable"));
        }
        observed++;
      }
      for (int index = observed; index < submitted.size(); index++)
        errors.add(
            new ErrorOutput(submitted.get(index), "Kinesis delivery acknowledgement unavailable"));
    } catch (Exception failure) {
      for (int index = observed; index < submitted.size(); index++)
        errors.add(
            new ErrorOutput(submitted.get(index), "Kinesis client error: " + failure.getMessage()));
    }
    return boundedResult(
        entries.size(),
        acknowledged,
        rejected + notAttempted,
        entries.size() - acknowledged - rejected,
        notAttempted,
        bytes,
        errors);
  }

  private static SimpleSinkCommand.FlushResult boundedResult(
      long attempted,
      long acknowledged,
      long rejected,
      long unknown,
      long notAttempted,
      long bytes,
      List<ErrorOutput> errors) {
    EffectOutcome.Delivery delivery =
        attempted == 0
            ? rejected > 0 ? EffectOutcome.Delivery.FAILED : EffectOutcome.Delivery.NOT_ATTEMPTED
            : acknowledged == attempted && notAttempted == 0
                ? EffectOutcome.Delivery.ACKNOWLEDGED
                : acknowledged > 0
                    ? EffectOutcome.Delivery.PARTIAL
                    : unknown > 0 ? EffectOutcome.Delivery.UNKNOWN : EffectOutcome.Delivery.FAILED;
    return new SimpleSinkCommand.FlushResult(
        (int) acknowledged,
        bytes,
        errors,
        new EffectOutcome(
            attempted,
            acknowledged,
            rejected,
            unknown,
            notAttempted,
            "kinesis_put_records_receipt",
            delivery));
  }

  @Override
  public void close() {
    kinesisClient.close();
  }
}
