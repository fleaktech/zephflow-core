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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.EmissionStrategy;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class LineEmissionStrategy implements EmissionStrategy {

  private final Charset charset;
  private final int batchSize;

  public LineEmissionStrategy(Charset charset, int batchSize) {
    if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be > 0");
    this.charset = charset;
    this.batchSize = batchSize;
  }

  @Override
  public void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx)
      throws Exception {
    try (InputStream in = reader.open(file.key(), 0);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, charset))) {
      List<RecordFleakData> batch = new ArrayList<>(batchSize);
      String line;
      while ((line = br.readLine()) != null) {
        batch.add((RecordFleakData) FleakData.wrap(Map.of("line", line, "file", file.key().urn())));
        if (batch.size() >= batchSize) {
          out.accept(batch);
          batch = new ArrayList<>(batchSize);
        }
      }
      if (!batch.isEmpty()) out.accept(batch);
    }
  }
}
