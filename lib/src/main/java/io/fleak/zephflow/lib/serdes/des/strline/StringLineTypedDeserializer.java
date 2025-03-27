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
package io.fleak.zephflow.lib.serdes.des.strline;

import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Created by bolei on 10/16/24 */
public class StringLineTypedDeserializer extends MultipleEventsTypedDeserializer<String> {
  @Override
  protected List<String> deserializeToMultipleTypedEvent(byte[] value) throws Exception {
    List<String> lines = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream(value), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    return lines;
  }
}
