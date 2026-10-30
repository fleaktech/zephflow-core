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

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.encoding;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.KvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import org.junit.jupiter.api.Test;

class EncodingConfigTest {

  @Test
  void parsesTheDecodingShapeAndASingleCompressionName() throws Exception {
    String schema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}";
    assertEquals(
        List.of(
            new EncodingConfig(
                List.of(),
                new CsvFormatConfig(
                    "|", null, null, false, List.of("ts", "user"), null, null, null, null)),
            new EncodingConfig(
                List.of(DecompressionCodec.GZIP),
                new JsonFormatConfig(JsonFormatConfig.Mode.LINES)),
            new EncodingConfig(
                List.of(DecompressionCodec.GZIP),
                new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY)),
            new EncodingConfig(null, new KvFormatConfig(List.of("a", "b"), "|", true)),
            new EncodingConfig(
                null, new ParquetFormatConfig(JsonUtils.OBJECT_MAPPER.readTree(schema)))),
        List.of(
            encoding(
                "{\"format\":{\"type\":\"csv\",\"delimiter\":\"|\",\"header\":false,"
                    + "\"columns\":[\"ts\",\"user\"]},\"compression\":[]}"),
            encoding(
                "{\"compression\":\"gzip\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"),
            encoding(
                "{\"compression\":[\"GZIP\"],\"format\":{\"type\":\"json\",\"mode\":\"array\"}}"),
            encoding(
                "{\"format\":{\"type\":\"kv\",\"fields\":[\"a\",\"b\"],\"pairDelimiter\":\"|\","
                    + "\"cleanFields\":true}}"),
            encoding("{\"format\":{\"type\":\"parquet\",\"schema\":" + schema + "}}")));
  }
}
