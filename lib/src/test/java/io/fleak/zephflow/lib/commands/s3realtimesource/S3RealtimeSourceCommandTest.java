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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceCommand.SourceType;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Exercises the full factory -> parse -> validate -> createExecutionContext wiring. */
class S3RealtimeSourceCommandTest {

  @Test
  void buildsStreamingExecutionContext() throws Exception {
    JobContext jobContext = TestUtils.buildJobContext(new HashMap<>());
    S3RealtimeSourceCommand command =
        new S3RealtimeSourceCommandFactory().createCommand("node", jobContext);

    Map<String, Object> config =
        Map.of(
            "queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789012/q",
            "regionStr", "us-east-1",
            "encodingType", "JSON_OBJECT_LINE");
    command.parseAndValidateArg(config);
    command.initialize(mock(MetricClientProvider.class));

    assertEquals(SourceType.STREAMING, command.sourceType());
    assertEquals("s3rtsource", command.commandName());
    assertInstanceOf(SourceExecutionContext.class, command.getExecutionContext());

    command.terminate();
  }
}
