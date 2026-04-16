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
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  public void close() {
    if (sqsClient != null) {
      sqsClient.close();
    }
  }
}
