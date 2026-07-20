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
package io.fleak.zephflow.lib.commands.s3filereader;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class S3FileReaderConfigParseTest {

  @Test
  void minimalConfigAppliesDefaults() {
    var parser = new JsonConfigParser<>(S3FileReaderDto.Config.class);
    S3FileReaderDto.Config c =
        parser.parseConfig(Map.of("pathField", "s3Path", "region", "us-east-1"));

    assertEquals("s3Path", c.getPathField());
    assertEquals("us-east-1", c.getRegion());
    assertEquals(S3FileReaderDto.Compression.AUTO, c.getCompression());
    assertEquals(S3FileReaderDto.Emission.LINE, c.getEmission());
    assertEquals(EncodingType.JSON_OBJECT_LINE, c.getEncodingType());
    assertTrue(c.isUrlDecodeKey());
    assertEquals(500, c.getBatchSize());
    assertNull(c.getCredentialId());
  }

  @Test
  void explicitValuesOverrideDefaults() {
    var parser = new JsonConfigParser<>(S3FileReaderDto.Config.class);
    S3FileReaderDto.Config c =
        parser.parseConfig(
            Map.of(
                "pathField", "p",
                "region", "us-west-2",
                "compression", "NONE",
                "emission", "DESERIALIZE",
                "urlDecodeKey", false));

    assertEquals(S3FileReaderDto.Compression.NONE, c.getCompression());
    assertEquals(S3FileReaderDto.Emission.DESERIALIZE, c.getEmission());
    assertFalse(c.isUrlDecodeKey());
  }
}
