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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.PausableSource;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.runner.RunStats.BufferState;
import io.fleak.zephflow.runner.RunStats.DrainState;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DrainControlTest {

  @TempDir Path dir;

  private final SourceCommand source =
      mock(SourceCommand.class, withSettings().extraInterfaces(PausableSource.class));
  private final PausableSource pausable = (PausableSource) source;
  private final AtomicReference<BufferState> buffer = new AtomicReference<>(BufferState.EMPTY);

  DrainControlTest() {
    when(pausable.supportsPause()).thenReturn(true);
  }

  private Path controlFile() {
    return dir.resolve("control");
  }

  private DrainControl control() {
    return new DrainControl(controlFile(), source, buffer::get);
  }

  private void write(String contents) throws Exception {
    Files.writeString(controlFile(), contents);
  }

  @Test
  void withoutAControlFileTheRunKeepsRunning() {
    DrainControl control = control();

    control.poll();

    assertEquals(DrainState.RUNNING, control.state());
    verify(pausable, never()).requestPause();
    verify(pausable, never()).requestResume();
  }

  @Test
  void drainsOnceTheSourceIsPausedAndTheBufferIsEmpty() throws Exception {
    DrainControl control = control();
    buffer.set(BufferState.PENDING);
    write(" drain\n");

    control.poll();
    assertEquals(DrainState.DRAINING, control.state());
    verify(pausable).requestPause();

    when(pausable.isPaused()).thenReturn(true);
    control.poll();
    assertEquals(DrainState.DRAINING, control.state(), "records are still buffered");

    buffer.set(BufferState.WAITING_FOR_SPACE);
    control.poll();
    assertEquals(DrainState.DRAINING, control.state());

    buffer.set(BufferState.EMPTY);
    control.poll();
    assertEquals(DrainState.DRAINED, control.state());
    verify(source).requestStop();
    verify(pausable).requestPause();
  }

  @Test
  void doesNotDrainBeforeTheSourceHasPaused() throws Exception {
    DrainControl control = control();
    write("drain");

    control.poll();
    control.poll();

    assertEquals(DrainState.DRAINING, control.state());
    verify(source, never()).requestStop();
  }

  @Test
  void resumeBeforeDrainedReturnsToRunning() throws Exception {
    DrainControl control = control();
    buffer.set(BufferState.PENDING);
    write("drain");
    control.poll();
    when(pausable.isPaused()).thenReturn(true);

    write("resume");
    control.poll();

    assertEquals(DrainState.RUNNING, control.state());
    verify(pausable).requestResume();
    verify(source, never()).requestStop();
  }

  @Test
  void removingTheControlFileResumes() throws Exception {
    DrainControl control = control();
    buffer.set(BufferState.PENDING);
    write("drain");
    control.poll();

    Files.delete(controlFile());
    control.poll();

    assertEquals(DrainState.RUNNING, control.state());
    verify(pausable).requestResume();
  }

  @Test
  void unknownContentsChangeNothing() throws Exception {
    DrainControl control = control();
    write("drain");
    control.poll();

    write("stop");
    control.poll();

    write("stop");
    control.poll();

    assertEquals(DrainState.DRAINING, control.state());
    verify(pausable, never()).requestResume();
  }

  @Test
  void anUnreadableControlFileChangesNothing() throws Exception {
    DrainControl control = control();
    write("drain");
    control.poll();

    Files.delete(controlFile());
    Files.createDirectory(controlFile());
    control.poll();

    assertEquals(DrainState.DRAINING, control.state());
    verify(pausable, never()).requestResume();
  }

  @Test
  void aSourceThatCannotPauseIgnoresDrain() throws Exception {
    when(pausable.supportsPause()).thenReturn(false);
    DrainControl control = control();
    write("drain");

    control.poll();
    control.poll();

    assertFalse(control.supportsDrain());
    assertEquals(DrainState.RUNNING, control.state());
    verify(pausable, never()).requestPause();
  }

  @Test
  void aSourceWithoutThePauseContractCannotDrain() {
    assertFalse(DrainControl.supportsDrain(mock(SourceCommand.class)));
  }

  @Test
  void drainedIsFinal() throws Exception {
    DrainControl control = control();
    when(pausable.isPaused()).thenReturn(true);
    write("drain");
    control.poll();
    assertEquals(DrainState.DRAINED, control.state());

    write("resume");
    control.poll();

    assertEquals(DrainState.DRAINED, control.state());
    verify(pausable, never()).requestResume();
  }

  @Test
  void pollsTheControlFileInTheBackground() throws Exception {
    when(pausable.isPaused()).thenReturn(true);
    try (DrainControl control = control()) {
      control.start(Duration.ofMillis(10));
      write("drain");

      verify(source, timeout(5_000)).requestStop();
      assertEquals(DrainState.DRAINED, control.state());
    }
  }

  @Test
  void aFailedBackgroundPollIsRetried() throws Exception {
    when(pausable.isPaused()).thenReturn(true);
    doThrow(new IllegalStateException("not ready")).doNothing().when(pausable).requestPause();
    write("drain");
    try (DrainControl control = control()) {
      control.start(Duration.ofMillis(10));

      verify(source, timeout(5_000)).requestStop();
      verify(pausable, times(2)).requestPause();
    }
  }
}
