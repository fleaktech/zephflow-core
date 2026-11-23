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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Frames per second of the automatic framer on 100,000 lines. Prints the rates; only JSON Lines has
 * a bound, far below what the framer reaches, so that a return of per-byte reads under the JSON
 * probe fails without timing noise failing the build.
 */
class AutoRecordFramerThroughputTest {
  private static final int LINES = 100_000;
  private static final double MINIMUM_JSON_LINES_FRAMES_PER_SECOND = 50_000;

  @Test
  void framesJsonLinesFasterThanTheBound() throws IOException {
    double jsonLines =
        measure(
            "JSON Lines",
            index ->
                "{\"id\":"
                    + index
                    + ",\"user\":\"user-"
                    + index
                    + "\",\"action\":\"login\",\"ok\":true,\"ms\":"
                    + index % 1000
                    + "}");
    measure(
        "mixed",
        index ->
            switch (index % 5) {
              case 0 -> "{\"id\":" + index + ",\"nested\":{\"values\":[1,2,3]}}";
              case 1 -> "<34>Oct 11 22:14:15 host app[" + index + "]: started";
              case 2 -> index + ",user-" + index + ",login,true";
              case 3 -> "user=user-" + index + " action=login ok=true";
              default -> "plain words for line " + index;
            });
    measure("CSV-like", index -> index + ",user-" + index + ",login,true," + index % 1000);
    assertTrue(
        jsonLines >= MINIMUM_JSON_LINES_FRAMES_PER_SECOND,
        "JSON Lines framed at " + Math.round(jsonLines) + " frames/s");
  }

  private static double measure(String name, IntFunction<String> line) throws IOException {
    byte[] content =
        IntStream.range(0, LINES)
            .mapToObj(line)
            .collect(Collectors.joining("\n", "", "\n"))
            .getBytes(StandardCharsets.UTF_8);
    frameAll(content);
    long start = System.nanoTime();
    int frames = frameAll(content);
    double seconds = (System.nanoTime() - start) / 1e9;
    assertEquals(LINES, frames);
    double rate = frames / seconds;
    System.out.printf("auto framer %s: %d frames/s%n", name, Math.round(rate));
    return rate;
  }

  private static int frameAll(byte[] content) throws IOException {
    var framer =
        new AutoRecordFramer(
            new ByteArrayInputStream(content), DecodeLimits.UNLIMITED, new DecodeFlags(), false);
    int frames = 0;
    while (framer.next() != null) frames++;
    return frames;
  }
}
