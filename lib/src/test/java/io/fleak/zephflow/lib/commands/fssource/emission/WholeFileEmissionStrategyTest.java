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

class WholeFileEmissionStrategyTest {

  @Test
  void emitsOneRecordWithFullContent() throws Exception {
    byte[] body = "hello world".getBytes(StandardCharsets.UTF_8);
    FileReader reader = (k, offset) -> new ByteArrayInputStream(body);
    FileEntry e =
        new FileEntry(new FileKey("file", "file:///t/x"), body.length, Instant.EPOCH, "x");

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

    new WholeFileEmissionStrategy(StandardCharsets.UTF_8)
        .emit(e, reader, acc, JobContext.builder().build());

    assertEquals(1, out.size());
    assertEquals("hello world", out.get(0).unwrap().get("content"));
    assertEquals("file:///t/x", out.get(0).unwrap().get("file"));
  }
}
