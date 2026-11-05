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
package io.fleak.zephflow.lib.serdes.encode;

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

record TextUnitWriter(boolean lineFeedAfterEachRecord) implements UnitWriter {

  @Override
  public byte[] write(List<RecordFleakData> records) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (RecordFleakData record : records) {
      bytes.writeBytes(text(record).getBytes(StandardCharsets.UTF_8));
      if (lineFeedAfterEachRecord) {
        bytes.write('\n');
      }
    }
    return bytes.toByteArray();
  }

  private static String text(RecordFleakData record) {
    FleakData raw = record.getPayload().get(FIELD_NAME_RAW);
    if (!(raw instanceof StringPrimitiveFleakData text)) {
      throw new IllegalArgumentException(
          "record has no string " + FIELD_NAME_RAW + " field to write as text");
    }
    return text.unwrap();
  }
}
