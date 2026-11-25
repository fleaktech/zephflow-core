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
package io.fleak.zephflow.lib.serdes.decode.auto;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class AutoRecordFramerInterruptTest {

  record FrameView(String text, long offset, long length) {}

  record Outcome(
      List<FrameView> frames,
      Class<?> failure,
      Class<?> markedReadFailure,
      int readsAfterInterrupt) {}

  @Test
  void stopsReadingATricklingSourceOnceInterrupted() {
    var source =
        new InterruptingTrickle(
            "{\"id\":1}\n{\"id\":2}\n".getBytes(StandardCharsets.UTF_8),
            ("{\"id\":3,\"pad\":\"" + "x".repeat(100_000)).getBytes(StandardCharsets.UTF_8));
    var flags = new DecodeFlags();
    var framer = new AutoRecordFramer(source, DecodeLimits.UNLIMITED, flags, false);
    List<FrameView> frames = new ArrayList<>();
    Class<?> failure = null;
    try {
      AutoRecordFramer.Frame frame;
      while ((frame = framer.next()) != null) {
        frames.add(
            new FrameView(
                new String(frame.bytes(), StandardCharsets.UTF_8), frame.offset(), frame.length()));
      }
    } catch (IOException exception) {
      failure = exception.getClass();
    } finally {
      Thread.interrupted();
    }

    assertEquals(
        new Outcome(List.of(), InterruptedIOException.class, InterruptedIOException.class, 0),
        new Outcome(
            frames,
            failure,
            flags.getReadFailure() == null ? null : flags.getReadFailure().getClass(),
            source.readsAfterInterrupt));
  }

  /**
   * Returns {@code complete} in one read, then {@code trickled} one byte per read; the first
   * trickled read interrupts the reading thread, as a stop would while the source is slow.
   */
  private static final class InterruptingTrickle extends InputStream {
    private final byte[] complete;
    private final byte[] trickled;
    private boolean completeDelivered;
    private int trickledPosition;
    private int readsAfterInterrupt = -1;

    private InterruptingTrickle(byte[] complete, byte[] trickled) {
      this.complete = complete;
      this.trickled = trickled;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
    }

    @Override
    public int read(byte[] target, int offset, int count) {
      if (count == 0) return 0;
      if (!completeDelivered) {
        completeDelivered = true;
        System.arraycopy(complete, 0, target, offset, complete.length);
        return complete.length;
      }
      if (trickledPosition == trickled.length) return -1;
      if (readsAfterInterrupt < 0) Thread.currentThread().interrupt();
      readsAfterInterrupt++;
      target[offset] = trickled[trickledPosition++];
      return 1;
    }
  }
}
