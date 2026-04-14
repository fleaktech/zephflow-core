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
package io.fleak.zephflow.lib.commands.sqssink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsSinkMessageProcessorTest {

  @Test
  void testPreprocessWithoutFifoFields() throws Exception {
    SqsSinkMessageProcessor processor = new SqsSinkMessageProcessor(null, null);

    RecordFleakData event =
        new RecordFleakData(
            Map.of(
                "name", new StringPrimitiveFleakData("test"),
                "value", new StringPrimitiveFleakData("123")));

    SqsOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertNotNull(result.body());
    assertTrue(result.body().contains("\"name\""));
    assertTrue(result.body().contains("\"test\""));
    assertNull(result.messageGroupId());
    assertNull(result.deduplicationId());
  }

  @Test
  void testPreprocessWithFifoFields() throws Exception {
    PathExpression groupIdExpression = PathExpression.fromString("$.groupId");
    PathExpression dedupIdExpression = PathExpression.fromString("$.dedupId");
    SqsSinkMessageProcessor processor =
        new SqsSinkMessageProcessor(groupIdExpression, dedupIdExpression);

    RecordFleakData event =
        new RecordFleakData(
            Map.of(
                "groupId", new StringPrimitiveFleakData("group-1"),
                "dedupId", new StringPrimitiveFleakData("dedup-1"),
                "data", new StringPrimitiveFleakData("payload")));

    SqsOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertNotNull(result.body());
    assertEquals("group-1", result.messageGroupId());
    assertEquals("dedup-1", result.deduplicationId());
  }

  @Test
  void testPreprocessWithMissingFifoFields() throws Exception {
    PathExpression groupIdExpression = PathExpression.fromString("$.groupId");
    PathExpression dedupIdExpression = PathExpression.fromString("$.dedupId");
    SqsSinkMessageProcessor processor =
        new SqsSinkMessageProcessor(groupIdExpression, dedupIdExpression);

    RecordFleakData event =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("payload")));

    SqsOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertNotNull(result.body());
    assertNull(result.messageGroupId());
    assertNull(result.deduplicationId());
  }

  @Test
  void testPreprocessBodyIsValidJson() throws Exception {
    SqsSinkMessageProcessor processor = new SqsSinkMessageProcessor(null, null);

    RecordFleakData event =
        new RecordFleakData(Map.of("key", new StringPrimitiveFleakData("value")));

    SqsOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertTrue(result.body().startsWith("{"));
    assertTrue(result.body().endsWith("}"));
    assertTrue(result.body().contains("\"key\""));
    assertTrue(result.body().contains("\"value\""));
  }
}
