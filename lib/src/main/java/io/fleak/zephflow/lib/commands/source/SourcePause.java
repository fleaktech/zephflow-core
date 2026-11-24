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
package io.fleak.zephflow.lib.commands.source;

import java.util.function.BooleanSupplier;

/**
 * Pause state shared by a source's control thread and its reading loop. The loop checks {@link
 * #requested()} between units of work and, once its progress is committed, calls {@link #hold}.
 */
public final class SourcePause {

  private final Object lock = new Object();
  private boolean requested;
  private volatile boolean paused;

  public void request() {
    synchronized (lock) {
      requested = true;
    }
  }

  public void resume() {
    synchronized (lock) {
      requested = false;
      lock.notifyAll();
    }
  }

  /** Wakes a held loop so it can see that it was stopped. */
  public void wake() {
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  public boolean requested() {
    synchronized (lock) {
      return requested;
    }
  }

  public boolean isPaused() {
    return paused;
  }

  /**
   * Reports the source as paused and blocks until resumed or {@code stopped}. Returns true when
   * resumed; a stopped source stays reported as paused.
   */
  public boolean hold(BooleanSupplier stopped) {
    boolean interrupted = false;
    try {
      synchronized (lock) {
        paused = true;
        while (requested && !stopped.getAsBoolean()) {
          try {
            lock.wait(1000);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
        if (stopped.getAsBoolean()) {
          return false;
        }
        paused = false;
        return true;
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
