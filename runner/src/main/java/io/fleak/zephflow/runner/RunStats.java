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
package io.fleak.zephflow.runner;

import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * What the run stats file carries. A null backlog, partition count or consumed count means unknown;
 * consumedRecords counts every record the source has consumed since the run started. The buffer is
 * PENDING while any sink's store-and-forward buffer holds records, and WAITING_FOR_SPACE while a
 * writer is blocked on a full one.
 */
public record RunStats(
    List<SourceStats> sources,
    BufferState bufferState,
    DrainState drainState,
    boolean supportsDrain) {

  public enum BufferState {
    EMPTY,
    PENDING,
    WAITING_FOR_SPACE
  }

  public enum DrainState {
    RUNNING,
    DRAINING,
    DRAINED
  }

  public record SourceStats(
      String nodeId,
      Long backlog,
      String backlogUnit,
      Integer partitionCount,
      Long consumedRecords) {}

  static RunStats collect(
      String sourceNodeId,
      SourceCommand source,
      Collection<? extends OperatorCommand> downstream,
      DrainState drainState,
      boolean supportsDrain,
      Long consumedRecords) {
    SourceBacklog backlog =
        contextOf(source) instanceof SourceExecutionContext<?> sourceContext
            ? sourceContext.fetcher().backlog()
            : SourceBacklog.UNKNOWN;
    return new RunStats(
        List.of(
            new SourceStats(
                sourceNodeId,
                backlog.amount(),
                backlog.unit().name().toLowerCase(Locale.ROOT),
                backlog.partitionCount(),
                consumedRecords)),
        bufferState(downstream),
        drainState,
        supportsDrain);
  }

  static BufferState bufferState(Collection<? extends OperatorCommand> downstream) {
    BufferState state = BufferState.EMPTY;
    for (OperatorCommand command : downstream) {
      if (!(contextOf(command) instanceof SinkExecutionContext<?> sinkContext)) {
        continue;
      }
      SinkStoreForward storeForward = sinkContext.storeForward();
      if (storeForward.isWaitingForSpace()) {
        return BufferState.WAITING_FOR_SPACE;
      }
      if (storeForward.isBuffering() || storeForward.pendingRecords() > 0) {
        state = BufferState.PENDING;
      }
    }
    return state;
  }

  private static ExecutionContext contextOf(OperatorCommand command) {
    try {
      return command.isInitialized() ? command.getExecutionContext() : null;
    } catch (IllegalStateException terminatedMeanwhile) {
      return null;
    }
  }
}
