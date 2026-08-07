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
import io.fleak.zephflow.lib.commands.sink.KeyExpressionResolver;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import javax.annotation.Nullable;

public class SqsSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<SqsOutboundMessage> {

  private final KeyExpressionResolver messageGroupIdResolver;
  private final KeyExpressionResolver deduplicationIdResolver;

  public SqsSinkMessageProcessor(
      @Nullable PathExpression messageGroupIdExpression,
      @Nullable PathExpression deduplicationIdExpression) {
    this.messageGroupIdResolver =
        new KeyExpressionResolver(
            messageGroupIdExpression,
            "messageGroupIdExpression",
            "such messages are sent without a message group id, which a FIFO queue rejects");
    this.deduplicationIdResolver =
        new KeyExpressionResolver(
            deduplicationIdExpression,
            "deduplicationIdExpression",
            "such messages are sent without a deduplication id and rely on the queue's"
                + " content-based deduplication, if enabled");
  }

  @Override
  public SqsOutboundMessage preprocess(RecordFleakData event, long ts) {
    String body = toJsonString(event);
    return new SqsOutboundMessage(
        body, messageGroupIdResolver.resolve(event), deduplicationIdResolver.resolve(event));
  }
}
