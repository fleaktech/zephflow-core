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

import io.fleak.zephflow.api.SourceCommand;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs the end-of-input flush when the JVM is asked to exit, e.g. when Grid stops a task with
 * SIGTERM. On SIGTERM the JVM runs only shutdown hooks, so without this the pipeline's pending
 * output (open windows, incomplete sample groups) never reaches the sinks.
 *
 * <p>The hook asks the source to stop and waits for the pipeline thread to finish, which flushes on
 * its way out. If the source cannot be stopped, or the pipeline does not finish within the timeout,
 * the hook flushes itself; {@link NoSourceDagRunner#terminate} is idempotent and, for a DAG with
 * stateful nodes, waits for an in-flight batch.
 */
@Slf4j
final class ShutdownFlushHook {

  // Grid force-kills the process 10s after SIGTERM.
  static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(8);

  private final SourceCommand sourceCommand;
  private final NoSourceDagRunner noSourceDagRunner;
  private final Duration timeout;
  private final CountDownLatch pipelineFinished = new CountDownLatch(1);
  private final Thread thread = new Thread(this::stopAndFlush, "zephflow-shutdown-flush");

  ShutdownFlushHook(
      SourceCommand sourceCommand, NoSourceDagRunner noSourceDagRunner, Duration timeout) {
    this.sourceCommand = sourceCommand;
    this.noSourceDagRunner = noSourceDagRunner;
    this.timeout = timeout;
  }

  void install() {
    Runtime.getRuntime().addShutdownHook(thread);
  }

  /** Called by the pipeline thread once it has finished and flushed, normally or not. */
  void pipelineFinished() {
    pipelineFinished.countDown();
    try {
      Runtime.getRuntime().removeShutdownHook(thread);
    } catch (IllegalStateException e) {
      // The JVM is already shutting down, so the hook is running and sees the latch.
    }
  }

  void stopAndFlush() {
    if (pipelineFinished.getCount() == 0) {
      return;
    }
    log.info("Shutdown requested: stopping the source and flushing pending output");
    boolean stopping = false;
    try {
      stopping = sourceCommand.requestStop();
    } catch (Exception e) {
      log.error("failed to stop the source", e);
    }
    try {
      if (stopping && pipelineFinished.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        log.info("Pipeline stopped and flushed its pending output");
        return;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (stopping) {
      log.warn("Pipeline did not stop within {}, flushing from the shutdown hook", timeout);
    } else {
      log.info(
          "Source {} cannot be stopped, flushing from the shutdown hook",
          sourceCommand.commandName());
    }
    noSourceDagRunner.terminate();
  }
}
