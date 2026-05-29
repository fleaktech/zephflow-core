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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FileReferenceEmissionStrategyTest {

  @Test
  void emitsSingleMetadataEvent() throws Exception {
    FileEntry entry =
        new FileEntry(
            new FileKey("s3", "s3://bkt/file_1700000000.json"),
            1234,
            Instant.parse("2026-01-01T00:00:00Z"),
            "s3://bkt/file_1700000000.json");

    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> records) {
            emitted.addAll(records);
          }

          @Override
          public void terminate() {}
        };

    new FileReferenceEmissionStrategy().emit(entry, null, out, JobContext.builder().build());

    assertEquals(1, emitted.size());
    Map<String, Object> payload = emitted.get(0).unwrap();
    assertEquals("s3://bkt/file_1700000000.json", payload.get("file"));
    assertEquals(1234L, ((Number) payload.get("size")).longValue());
    assertNotNull(payload.get("lastModified"));
  }
}
