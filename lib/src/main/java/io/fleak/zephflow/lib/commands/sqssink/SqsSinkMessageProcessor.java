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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import javax.annotation.Nullable;

public class SqsSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<SqsOutboundMessage> {

  private final PathExpression messageGroupIdExpression;
  private final PathExpression deduplicationIdExpression;

  public SqsSinkMessageProcessor(
      @Nullable PathExpression messageGroupIdExpression,
      @Nullable PathExpression deduplicationIdExpression) {
    this.messageGroupIdExpression = messageGroupIdExpression;
    this.deduplicationIdExpression = deduplicationIdExpression;
  }

  @Override
  public SqsOutboundMessage preprocess(RecordFleakData event, long ts) {
    String body = toJsonString(event);

    String messageGroupId = null;
    if (messageGroupIdExpression != null) {
      messageGroupId = messageGroupIdExpression.getStringValueFromEventOrDefault(event, null);
    }

    String deduplicationId = null;
    if (deduplicationIdExpression != null) {
      deduplicationId = deduplicationIdExpression.getStringValueFromEventOrDefault(event, null);
    }

    return new SqsOutboundMessage(body, messageGroupId, deduplicationId);
  }
}
