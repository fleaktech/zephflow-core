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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.SourceCommand;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ShutdownFlushHookTest {

  private final SourceCommand source = mock(SourceCommand.class);
  private final NoSourceDagRunner runner = mock(NoSourceDagRunner.class);
  private final ShutdownFlushHook hook =
      new ShutdownFlushHook(source, runner, Duration.ofMillis(200));

  @Test
  void stoppedPipeline_flushesOnItsOwnThread() {
    when(source.requestStop())
        .thenAnswer(
            invocation -> {
              hook.pipelineFinished();
              return true;
            });

    hook.stopAndFlush();

    verify(runner, never()).terminate();
  }

  @Test
  void pipelineNotStoppingInTime_isFlushedByTheHook() {
    when(source.requestStop()).thenReturn(true);

    hook.stopAndFlush();

    verify(runner).terminate();
  }

  @Test
  void unstoppableSource_isFlushedByTheHookWithoutWaiting() {
    when(source.requestStop()).thenReturn(false);
    ShutdownFlushHook slowHook = new ShutdownFlushHook(source, runner, Duration.ofHours(1));

    slowHook.stopAndFlush();

    verify(runner).terminate();
  }

  @Test
  void finishedPipeline_isLeftAlone() {
    hook.pipelineFinished();

    hook.stopAndFlush();

    verify(source, never()).requestStop();
    verify(runner, never()).terminate();
  }
}
