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

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/24/25 */
class StringLineDeserializerFactoryTest {
  @Test
  void createDeserializer() throws Exception {
    byte[] raw;
    try (InputStream is =
        this.getClass().getResourceAsStream("/parser/windows_multiline_4624.txt")) {
      Preconditions.checkNotNull(is);
      raw = IOUtils.toByteArray(is);
    }
    var desFac = DeserializerFactory.createDeserializerFactory(EncodingType.STRING_LINE);
    var deser = desFac.createDeserializer();
    List<RecordFleakData> actual = deser.deserialize(new SerializedEvent(null, raw, null));
    assertEquals(64, actual.size());
  }
}
