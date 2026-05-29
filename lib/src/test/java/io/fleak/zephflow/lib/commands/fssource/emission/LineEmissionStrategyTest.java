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
package io.fleak.zephflow.lib.commands.fssource.emission;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LineEmissionStrategyTest {

  private static FileReader readerOf(byte[] bytes) {
    return (k, offset) -> new ByteArrayInputStream(bytes);
  }

  @Test
  void emitsOneRecordPerLine() throws Exception {
    String body = "line-1\nline-2\nline-3";
    FileEntry e =
        new FileEntry(new FileKey("file", "file:///t/x"), body.length(), Instant.EPOCH, "x");

    List<RecordFleakData> out = new ArrayList<>();
    SourceEventAcceptor acc =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            out.addAll(r);
          }

          @Override
          public void terminate() {}
        };

    new LineEmissionStrategy(StandardCharsets.UTF_8, 100)
        .emit(
            e, readerOf(body.getBytes(StandardCharsets.UTF_8)), acc, JobContext.builder().build());

    assertEquals(3, out.size());
    assertEquals("line-1", out.get(0).unwrap().get("line"));
    assertEquals("line-3", out.get(2).unwrap().get("line"));
  }

  @Test
  void batchesByConfiguredSize() throws Exception {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 250; i++) b.append("L").append(i).append("\n");
    FileEntry e = new FileEntry(new FileKey("file", "file:///t/x"), b.length(), Instant.EPOCH, "x");

    int[] batchCount = {0};
    SourceEventAcceptor acc =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            batchCount[0]++;
          }

          @Override
          public void terminate() {}
        };

    new LineEmissionStrategy(StandardCharsets.UTF_8, 100)
        .emit(
            e,
            readerOf(b.toString().getBytes(StandardCharsets.UTF_8)),
            acc,
            JobContext.builder().build());

    assertEquals(3, batchCount[0]); // 100 + 100 + 50
  }
}
