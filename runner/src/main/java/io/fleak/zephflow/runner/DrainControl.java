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

import io.fleak.zephflow.api.PausableSource;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.runner.RunStats.BufferState;
import io.fleak.zephflow.runner.RunStats.DrainState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Lets a supervisor pause a running pipeline through a control file. {@code drain} pauses the
 * source; once it has committed its progress and every store-and-forward buffer is empty, the run
 * counts as drained and the source is stopped, which ends the run. {@code resume}, or removing the
 * file, resumes a source that has not drained yet.
 */
@Slf4j
final class DrainControl implements AutoCloseable {

  static final Duration POLL_INTERVAL = Duration.ofSeconds(1);
  static final String DRAIN = "drain";
  static final String RESUME = "resume";

  private final Path controlFile;
  private final SourceCommand source;
  private final Supplier<BufferState> bufferState;
  private volatile DrainState state = DrainState.RUNNING;
  private boolean warnedUnsupported;
  private String lastUnknownContents;
  private ScheduledExecutorService scheduler;

  DrainControl(Path controlFile, SourceCommand source, Supplier<BufferState> bufferState) {
    this.controlFile = controlFile;
    this.source = source;
    this.bufferState = bufferState;
  }

  static boolean supportsDrain(SourceCommand source) {
    return source instanceof PausableSource pausable && pausable.supportsPause();
  }

  boolean supportsDrain() {
    return supportsDrain(source);
  }

  DrainState state() {
    return state;
  }

  /** Starts polling; a null control file leaves the pipeline running as it is. */
  void start(Duration interval) {
    if (controlFile == null) {
      return;
    }
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "drain-control");
              thread.setDaemon(true);
              return thread;
            });
    scheduler.scheduleWithFixedDelay(
        this::pollSafely, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void pollSafely() {
    try {
      poll();
    } catch (Exception e) {
      log.warn("Failed to apply the control file {}", controlFile, e);
    }
  }

  synchronized void poll() {
    if (state == DrainState.DRAINED) {
      return;
    }
    String command = readCommand();
    if (DRAIN.equals(command)) {
      startDraining();
    } else if (RESUME.equals(command) && state == DrainState.DRAINING) {
      ((PausableSource) source).requestResume();
      state = DrainState.RUNNING;
      log.info("Resuming the source");
    }
    if (state == DrainState.DRAINING
        && ((PausableSource) source).isPaused()
        && bufferState.get() == BufferState.EMPTY) {
      state = DrainState.DRAINED;
      log.info("Source paused with its progress committed and buffers empty; ending the run");
      source.requestStop();
    }
  }

  private void startDraining() {
    if (state != DrainState.RUNNING) {
      return;
    }
    if (!supportsDrain()) {
      if (!warnedUnsupported) {
        warnedUnsupported = true;
        log.warn("Ignoring drain: source {} cannot pause", source.commandName());
      }
      return;
    }
    ((PausableSource) source).requestPause();
    state = DrainState.DRAINING;
    log.info("Draining: pausing the source and waiting for buffers to empty");
  }

  /** An absent file reads as resume; an unreadable one or unknown contents change nothing. */
  private String readCommand() {
    String contents;
    try {
      contents = Files.readString(controlFile).trim();
    } catch (NoSuchFileException e) {
      return RESUME;
    } catch (IOException e) {
      log.warn("Failed to read the control file {}", controlFile, e);
      return null;
    }
    if (!DRAIN.equals(contents) && !RESUME.equals(contents)) {
      if (!contents.equals(lastUnknownContents)) {
        lastUnknownContents = contents;
        log.warn("Ignoring unknown control file contents: {}", contents);
      }
      return null;
    }
    return contents;
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
}
