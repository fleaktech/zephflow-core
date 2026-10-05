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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.execution.ExecutionObserver.*;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;

/** Serializes observations, including callbacks from an operator's existing refresh worker. */
final class BoundedExecution {
  final BoundedDefinition definition;
  private final ExecutionObserver observer;
  private final ExecutionControl control;
  private final ThreadLocal<Invocation> activeInvocation = new ThreadLocal<>();
  private long sequence;
  private long invocationCount;
  private long failedInvocationCount;
  private long recordErrorCount;
  private long effectCount;
  private volatile ExecutionStoppedException stopped;

  BoundedExecution(
      BoundedDefinition definition, ExecutionObserver observer, ExecutionControl control) {
    this.definition = definition;
    this.observer = Objects.requireNonNull(observer);
    this.control = Objects.requireNonNull(control);
  }

  void checkpoint() {
    if (stopped != null) throw stopped;
    try {
      control.checkpoint();
    } catch (ExecutionStoppedException failure) {
      stopped = failure;
      throw failure;
    } catch (RuntimeException failure) {
      stopped = new ExecutionStoppedException("Execution control stopped the run", failure);
      throw stopped;
    }
  }

  private synchronized void emit(LongConsumer callback) {
    try {
      callback.accept(++sequence);
    } catch (ExecutionStoppedException failure) {
      stopped = failure;
      throw failure;
    } catch (RuntimeException failure) {
      stopped = new ExecutionStoppedException("Execution observation failed", failure);
      throw stopped;
    }
  }

  synchronized Invocation start(String nodeId, String upstreamId, Phase phase, String bindingId) {
    Invocation invocation = new Invocation(++invocationCount, nodeId, upstreamId, phase, bindingId);
    emit(sequence -> observer.invocationStarted(sequence, invocation));
    return invocation;
  }

  void records(Invocation invocation, Side side, List<RecordFleakData> records) {
    emit(sequence -> observer.records(sequence, invocation, side, records));
  }

  synchronized void errors(Invocation invocation, List<ErrorOutput> errors) {
    if (errors.isEmpty()) return;
    recordErrorCount += errors.size();
    emit(sequence -> observer.recordErrors(sequence, invocation, errors));
  }

  synchronized void failed(Invocation invocation, Throwable failure) {
    failedInvocationCount++;
    emit(sequence -> observer.invocationFailed(sequence, invocation, failure));
  }

  void finish(Invocation invocation, long input, long output, boolean complete) {
    emit(sequence -> observer.invocationFinished(sequence, invocation, input, output, complete));
  }

  Invocation enter(Invocation invocation) {
    Invocation previous = activeInvocation.get();
    activeInvocation.set(invocation);
    return previous;
  }

  void restore(Invocation previous) {
    if (previous == null) activeInvocation.remove();
    else activeInvocation.set(previous);
  }

  ExecutionHooks hooks(String nodeId) {
    return new ExecutionHooks(
        this::checkpoint,
        new ExecutionHooks.Effects() {
          @Override
          public long started() {
            checkpoint();
            Invocation invocation =
                Objects.requireNonNull(activeInvocation.get(), "No active invocation");
            synchronized (BoundedExecution.this) {
              long effectId = ++effectCount;
              emit(sequence -> observer.effectStarted(sequence, invocation, effectId));
              return effectId;
            }
          }

          @Override
          public void finished(long effectId, EffectOutcome outcome) {
            Invocation invocation =
                Objects.requireNonNull(activeInvocation.get(), "No active invocation");
            emit(sequence -> observer.effectFinished(sequence, invocation, effectId, outcome));
          }
        },
        work -> {
          checkpoint();
          Invocation invocation = start(nodeId, null, Phase.BACKGROUND, null);
          Invocation previous = enter(invocation);
          try {
            long effect = hooks(nodeId).effects().started();
            boolean returnedSuccessfully = false;
            try {
              work.run();
              returnedSuccessfully = true;
            } finally {
              hooks(nodeId).effects().finished(effect, EffectOutcome.opaque(returnedSuccessfully));
            }
            checkpoint();
            finish(invocation, 0, 0, true);
          } catch (ExecutionStoppedException failure) {
            finish(invocation, 0, 0, false);
            throw failure;
          } catch (RuntimeException failure) {
            failed(invocation, failure);
            finish(invocation, 0, 0, false);
            throw failure;
          } finally {
            restore(previous);
          }
        });
  }

  synchronized BoundedRunResult summary() {
    return new BoundedRunResult(invocationCount, failedInvocationCount, recordErrorCount);
  }
}
