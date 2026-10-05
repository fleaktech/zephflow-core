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
package io.fleak.zephflow.lib.commands.sqssink;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BoundedFlushException;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

@Slf4j
public class SqsSinkFlusher implements SimpleSinkCommand.Flusher<SqsOutboundMessage> {

  private final SqsClient sqsClient;
  private final String queueUrl;

  public SqsSinkFlusher(SqsClient sqsClient, String queueUrl) {
    this.sqsClient = sqsClient;
    this.queueUrl = queueUrl;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {

    List<SqsOutboundMessage> messages = preparedInputEvents.preparedList();
    if (messages.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<SendMessageBatchRequestEntry> entries = new ArrayList<>(messages.size());
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    List<Integer> messageSizes = new ArrayList<>();

    for (int i = 0; i < messages.size(); i++) {
      SqsOutboundMessage message = messages.get(i);
      RecordFleakData rawEvent = preparedInputEvents.rawAndPreparedList().get(i).getLeft();

      try {
        SendMessageBatchRequestEntry.Builder entryBuilder =
            SendMessageBatchRequestEntry.builder()
                .id(String.valueOf(i))
                .messageBody(message.body());

        if (message.messageGroupId() != null) {
          entryBuilder.messageGroupId(message.messageGroupId());
        }
        if (message.deduplicationId() != null) {
          entryBuilder.messageDeduplicationId(message.deduplicationId());
        }

        entries.add(entryBuilder.build());
        messageSizes.add(message.body().getBytes(StandardCharsets.UTF_8).length);
      } catch (Exception e) {
        errorOutputs.add(
            new ErrorOutput(rawEvent, "Failed to prepare SQS message: " + e.getMessage()));
        messageSizes.add(0);
      }
    }

    if (entries.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
    }

    SendMessageBatchRequest batchRequest =
        SendMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build();

    try {
      SendMessageBatchResponse response = sqsClient.sendMessageBatch(batchRequest);

      int successCount = response.successful() != null ? response.successful().size() : 0;
      long flushedDataSize = 0;

      if (response.successful() != null) {
        for (var success : response.successful()) {
          int index = Integer.parseInt(success.id());
          flushedDataSize += messageSizes.get(index);
        }
      }

      if (response.failed() != null) {
        for (BatchResultErrorEntry failed : response.failed()) {
          int index = Integer.parseInt(failed.id());
          RecordFleakData rawEvent = preparedInputEvents.rawAndPreparedList().get(index).getLeft();
          errorOutputs.add(
              new ErrorOutput(
                  rawEvent,
                  String.format(
                      "SQS batch send failed: %s - %s", failed.code(), failed.message())));
        }
      }

      log.debug(
          "SQS flush completed: {} successful, {} failed",
          successCount,
          response.failed() != null ? response.failed().size() : 0);

      return new SimpleSinkCommand.FlushResult(successCount, flushedDataSize, errorOutputs);
    } catch (Exception e) {
      log.error("SQS batch send failed", e);
      for (Pair<RecordFleakData, SqsOutboundMessage> pair :
          preparedInputEvents.rawAndPreparedList()) {
        errorOutputs.add(new ErrorOutput(pair.getLeft(), "SQS client error: " + e.getMessage()));
      }
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
    }
  }

  @Override
  public SimpleSinkCommand.FlushResult flushBounded(
      SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> events,
      Map<String, String> metricTags,
      ExecutionHooks hooks) {
    List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
    Map<String, RecordFleakData> submitted = new LinkedHashMap<>();
    Map<String, Integer> sizes = new LinkedHashMap<>();
    List<ErrorOutput> errors = new ArrayList<>();
    for (int index = 0; index < events.rawAndPreparedList().size(); index++) {
      var pair = events.rawAndPreparedList().get(index);
      try {
        var message = pair.getRight();
        String id = String.valueOf(index);
        int size = message.body().getBytes(StandardCharsets.UTF_8).length;
        var entry =
            SendMessageBatchRequestEntry.builder()
                .id(id)
                .messageBody(message.body())
                .messageGroupId(message.messageGroupId())
                .messageDeduplicationId(message.deduplicationId())
                .build();
        entries.add(entry);
        submitted.put(id, pair.getLeft());
        sizes.put(id, size);
      } catch (Exception failure) {
        errors.add(
            new ErrorOutput(
                pair.getLeft(), "Failed to prepare SQS message: " + failure.getMessage()));
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
    Set<String> acknowledged = new HashSet<>();
    Set<String> rejected = new HashSet<>();
    try {
      var response =
          sqsClient.sendMessageBatch(
              SendMessageBatchRequest.builder().queueUrl(queueUrl).entries(entries).build());
      if (response == null)
        throw new IllegalStateException("Received null response from SQS client");
      for (var success : response.successful()) {
        if (submitted.containsKey(success.id())) acknowledged.add(success.id());
      }
      for (var failure : response.failed()) {
        if (submitted.containsKey(failure.id())
            && !acknowledged.contains(failure.id())
            && rejected.add(failure.id()))
          errors.add(
              new ErrorOutput(
                  submitted.get(failure.id()),
                  "SQS batch send failed: " + failure.code() + " - " + failure.message()));
      }
      for (var item : submitted.entrySet()) {
        if (!acknowledged.contains(item.getKey()) && !rejected.contains(item.getKey()))
          errors.add(new ErrorOutput(item.getValue(), "SQS delivery acknowledgement unavailable"));
      }
    } catch (Exception failure) {
      for (var item : submitted.entrySet()) {
        if (!acknowledged.contains(item.getKey()) && !rejected.contains(item.getKey()))
          errors.add(new ErrorOutput(item.getValue(), "SQS client error: " + failure.getMessage()));
      }
    }
    long bytes = acknowledged.stream().mapToLong(sizes::get).sum();
    return boundedResult(
        entries.size(),
        acknowledged.size(),
        rejected.size() + notAttempted,
        entries.size() - acknowledged.size() - rejected.size(),
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
            "sqs_send_message_batch_receipt",
            delivery));
  }

  @Override
  public void close() {
    if (sqsClient != null) {
      sqsClient.close();
    }
  }
}
