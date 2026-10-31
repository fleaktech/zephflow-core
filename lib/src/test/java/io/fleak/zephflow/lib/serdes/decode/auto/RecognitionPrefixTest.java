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
package io.fleak.zephflow.lib.serdes.decode.auto;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import io.fleak.zephflow.lib.serdes.decode.yaml.YamlRecognition;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RecognitionPrefixTest {
  @Test
  void probesReplayTheSamePrefixAcrossShortReads() throws Exception {
    String text = "\n\n---\na: 1\n---\nb: 2\n";
    try (var prefix =
        new RecognitionPrefix(
            PeekedStream.peek(
                new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)),
                8,
                head -> head.length == 8),
            1024,
            new DecodeFlags())) {
      assertEquals("\n\n-", new String(prefix.probe().readNBytes(3), StandardCharsets.UTF_8));
      assertTrue(YamlRecognition.candidate(prefix.probe()));
      assertEquals(2, YamlRecognition.documentCount(prefix.probe(), 1024));
      assertEquals(text, new String(prefix.replay().readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}
