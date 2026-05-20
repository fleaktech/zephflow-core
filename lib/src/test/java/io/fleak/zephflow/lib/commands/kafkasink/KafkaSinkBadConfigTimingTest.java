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
package io.fleak.zephflow.lib.commands.kafkasink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * FLE-840: a test run against an unreachable / misconfigured Kafka broker must fail fast with a
 * real error, instead of blocking {@code max.block.ms} per event (~{@code N * 10s}, which trips the
 * API gateway timeout) and then silently reporting success.
 */
@Slf4j
class KafkaSinkBadConfigTimingTest {

  // Closed local port: connections are refused, metadata never resolves.
  private static final String UNREACHABLE_BROKER = "localhost:1";

  @Test
  void testRun_badBroker_failsFastWithRealError() {
    Map<String, Serializable> testModeProps = new HashMap<>();
    testModeProps.put(JobContext.FLAG_TEST_MODE, true);
    JobContext jobContext = TestUtils.buildJobContext(testModeProps);

    KafkaSinkCommand command =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory().createCommand("bad_kafka_node", jobContext);

    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic("bolei_output")
            .broker(UNREACHABLE_BROKER)
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .build();
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    long start = System.currentTimeMillis();
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> command.initialize(new MetricClientProvider.NoopMetricClientProvider()));
    long elapsedMs = System.currentTimeMillis() - start;

    log.info("FLE-840: failed in {} ms with: {}", elapsedMs, ex.getMessage());

    // A single bounded metadata lookup, not one max.block.ms wait per event.
    assertTrue(elapsedMs < 20_000, "expected fail-fast (<20s) but took " + elapsedMs + " ms");
    assertTrue(ex.getMessage().contains(UNREACHABLE_BROKER), ex.getMessage());
    assertTrue(ex.getMessage().contains("bolei_output"), ex.getMessage());
  }
}
