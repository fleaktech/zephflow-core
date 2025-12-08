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
package io.fleak.zephflow.lib.commands.stdin;

import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/** Created by bolei on 12/20/24 */
@Slf4j
public class StdInSourceFetcher implements Fetcher<SerializedEvent> {

  private volatile boolean exhausted = false;

  @Override
  public List<SerializedEvent> fetch() {
    log.debug("Waiting for event from stdin...");
    StringBuilder inputData = new StringBuilder();
    System.out.println("use an empty line to quit");
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
      String line;
      while ((line = reader.readLine()) != null && !line.trim().isEmpty()) {
        inputData.append(line);
      }
      log.debug("read data from stdin:\n {}", inputData);
      byte[] raw = inputData.toString().getBytes();
      SerializedEvent serializedEvent = new SerializedEvent(null, raw, null);
      exhausted = true;
      return List.of(serializedEvent);
    } catch (Exception e) {
      log.error("failed to read data from stdin", e);
      return null;
    }
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public void close() throws IOException {}
}
