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
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * What the run stats file carries. A null backlog or partition count means unknown. The buffer is
 * PENDING while any sink is writing to its store-and-forward buffer.
 */
public record RunStats(
    List<SourceStats> sources,
    BufferState bufferState,
    DrainState drainState,
    boolean supportsDrain) {

  public enum BufferState {
    EMPTY,
    PENDING
  }

  public enum DrainState {
    RUNNING
  }

  public record SourceStats(
      String nodeId, Long backlog, String backlogUnit, Integer partitionCount) {}

  static RunStats collect(
      String sourceNodeId, SourceCommand source, Collection<? extends OperatorCommand> downstream) {
    SourceBacklog backlog =
        contextOf(source) instanceof SourceExecutionContext<?> sourceContext
            ? sourceContext.fetcher().backlog()
            : SourceBacklog.UNKNOWN;
    boolean buffering =
        downstream.stream()
            .anyMatch(
                command ->
                    contextOf(command) instanceof SinkExecutionContext<?> sinkContext
                        && sinkContext.storeForward().isBuffering());
    return new RunStats(
        List.of(
            new SourceStats(
                sourceNodeId,
                backlog.amount(),
                backlog.unit().name().toLowerCase(Locale.ROOT),
                backlog.partitionCount())),
        buffering ? BufferState.PENDING : BufferState.EMPTY,
        DrainState.RUNNING,
        false);
  }

  private static ExecutionContext contextOf(OperatorCommand command) {
    try {
      return command.isInitialized() ? command.getExecutionContext() : null;
    } catch (IllegalStateException terminatedMeanwhile) {
      return null;
    }
  }
}
