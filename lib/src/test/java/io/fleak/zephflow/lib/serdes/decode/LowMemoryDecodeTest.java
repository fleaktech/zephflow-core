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
package io.fleak.zephflow.lib.serdes.decode;

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.decoder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Runs in the lowMemoryTest task with a 64 MiB heap. Every input here is several times larger than
 * the heap and is generated on the fly, so a decoder that buffers the whole input fails with OOM.
 */
@Tag("lowMemory")
class LowMemoryDecodeTest {

  private static final long TARGET_BYTES = 300L * 1024 * 1024;
  private static final DecodeLimits LIMITS = new DecodeLimits(1024 * 1024, 4 * 1024 * 1024);
  private static final String PAD = "x".repeat(200);

  @BeforeAll
  static void heapIsSmallerThanTheInputs() {
    assertTrue(
        Runtime.getRuntime().maxMemory() < TARGET_BYTES / 2,
        "run through the lowMemoryTest task so the heap is bounded");
  }

  @Test
  void jsonLinesFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input =
        generated(records, i -> "{\"i\":" + i + ",\"p\":\"" + PAD + "\"}\n", "", "");
    assertEquals(
        Map.of("Decoded", records),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"), input, LIMITS));
  }

  @Test
  void jsonArrayFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input =
        generated(
            records,
            i -> (i == 0 ? "" : ",") + "{\"i\":" + i + ",\"p\":\"" + PAD + "\"}",
            "[",
            "]");
    assertEquals(
        Map.of("Decoded", records),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"array\"}}"), input, LIMITS));
  }

  @Test
  void gzippedTextLinesFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 210;
    InputStream input = gzipped(generated(records, i -> PAD + i + "\n", "", ""));
    assertEquals(
        Map.of("Decoded", records),
        count(
            decoder(
                "{\"compression\":\"gzip\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}"),
            input,
            LIMITS));
  }

  @Test
  void hugeSingleLineIsOversizedAndTheNextLineStillDecodes() {
    InputStream input = generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "", "\n{\"ok\":1}\n");
    assertEquals(
        Map.of("Oversized", 1L, "Decoded", 1L),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"), input, LIMITS));
  }

  @Test
  void hugeArrayElementIsOversized() {
    InputStream input =
        generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "[{\"ok\":1},{\"s\":\"", "\"}]");
    assertEquals(
        Map.of("Decoded", 1L, "Oversized", 1L),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"array\"}}"), input, LIMITS));
  }

  @Test
  void hugeDocumentIsOversized() {
    InputStream input = generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "{\"s\":\"", "\"}");
    assertEquals(
        Map.of("Oversized", 1L),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"object\"}}"), input, LIMITS));
  }

  private static Map<String, Long> count(
      RecordDecoder decoder, InputStream input, DecodeLimits limits) {
    Map<String, Long> counts = new LinkedHashMap<>();
    try (RecordCursor cursor = decoder.open(() -> input, limits)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        counts.merge(outcome.getClass().getSimpleName(), 1L, Long::sum);
      }
    } catch (SourceReadException e) {
      throw new UncheckedIOException(e);
    }
    return counts;
  }

  private static InputStream generated(
      long count, LongFunction<String> item, String prefix, String suffix) {
    return new InputStream() {
      private long next;
      private byte[] current = prefix.getBytes(StandardCharsets.UTF_8);
      private int pos;
      private boolean suffixDone;

      @Override
      public int read() {
        byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
      }

      @Override
      public int read(byte[] b, int off, int len) {
        while (pos == current.length) {
          if (next < count) {
            current = item.apply(next++).getBytes(StandardCharsets.UTF_8);
          } else if (!suffixDone) {
            suffixDone = true;
            current = suffix.getBytes(StandardCharsets.UTF_8);
          } else {
            return -1;
          }
          pos = 0;
        }
        int n = Math.min(len, current.length - pos);
        System.arraycopy(current, pos, b, off, n);
        pos += n;
        return n;
      }
    };
  }

  private static InputStream gzipped(InputStream plain) {
    try {
      PipedInputStream in = new PipedInputStream(64 * 1024);
      OutputStream out = new PipedOutputStream(in);
      Thread writer =
          new Thread(
              () -> {
                try (GZIPOutputStream gz = new GZIPOutputStream(out, 64 * 1024)) {
                  plain.transferTo(gz);
                } catch (IOException ignored) {
                  // The reader closed early.
                }
              });
      writer.setDaemon(true);
      writer.start();
      return in;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
