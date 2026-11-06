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
package io.fleak.zephflow.lib.commands.sink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SinkExecutionContextTest {

  @Test
  void closeStopsReplayThenClosesTheFlusherThenTheStore() throws Exception {
    List<String> calls = new ArrayList<>();

    context(calls, false).close();

    assertEquals(List.of("stopReplay", "flusher.close", "store.close"), calls);
  }

  @Test
  void abortStopsReplayThenAbortsTheFlusherThenClosesTheStore() throws Exception {
    List<String> calls = new ArrayList<>();

    context(calls, false).abort();

    assertEquals(List.of("stopReplay", "flusher.abort", "store.close"), calls);
  }

  @Test
  void storeClosesWhenTheFlusherFailsToClose() {
    List<String> calls = new ArrayList<>();

    assertThrows(IOException.class, () -> context(calls, true).close());

    assertEquals(List.of("stopReplay", "flusher.close", "store.close"), calls);
  }

  private static SinkExecutionContext<RecordFleakData> context(
      List<String> calls, boolean flusherFails) {
    SimpleSinkCommand.Flusher<RecordFleakData> flusher =
        new SimpleSinkCommand.Flusher<>() {
          @Override
          public SimpleSinkCommand.FlushResult flush(
              SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events,
              Map<String, String> tags) {
            return new SimpleSinkCommand.FlushResult(0, 0, List.of());
          }

          @Override
          public void abort() {
            calls.add("flusher.abort");
          }

          @Override
          public void close() throws IOException {
            calls.add("flusher.close");
            if (flusherFails) {
              throw new IOException("close failed");
            }
          }
        };
    SinkStoreForward store =
        new SinkStoreForward() {
          @Override
          public boolean isBuffering() {
            return false;
          }

          @Override
          public boolean shouldBuffer(Throwable t) {
            return false;
          }

          @Override
          public int offer(List<RecordFleakData> records) {
            return 0;
          }

          @Override
          public void start(ReplayTarget target) {}

          @Override
          public void stopReplay() {
            calls.add("stopReplay");
          }

          @Override
          public void close() {
            calls.add("store.close");
          }
        };
    return new SinkExecutionContext<>(
        flusher, (event, ts) -> event, store, null, null, null, null, null);
  }
}
