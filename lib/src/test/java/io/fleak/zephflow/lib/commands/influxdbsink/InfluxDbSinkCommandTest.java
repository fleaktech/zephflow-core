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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InfluxDbSinkCommandTest {

  private JobContext jobContext;
  private final InfluxDbClientProvider clientProvider = mock(InfluxDbClientProvider.class);

  @BeforeEach
  void setUp() {
    jobContext = new JobContext();
    jobContext.setMetricTags(TestUtils.JOB_CONTEXT.getMetricTags());
    jobContext.setOtherProperties(Map.of());
  }

  private InfluxDbSinkCommand command(InfluxDbSinkDto.Config config) {
    InfluxDbSinkCommand command =
        (InfluxDbSinkCommand)
            new InfluxDbSinkCommandFactory(clientProvider).createCommand("node", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    return command;
  }

  private static InfluxDbSinkDto.Config.ConfigBuilder validConfig() {
    return InfluxDbSinkDto.Config.builder()
        .url("http://localhost:8086")
        .org("org")
        .bucket("bucket")
        .measurement("m");
  }

  @Test
  void reportsCommandName() {
    assertEquals("influxdbsink", command(validConfig().build()).commandName());
  }

  @Test
  void usesConfiguredBatchSize() {
    assertEquals(50, command(validConfig().batchSize(50).build()).batchSize());
  }

  @Test
  void appliesDefaultBatchSize() {
    assertEquals(InfluxDbSinkDto.DEFAULT_BATCH_SIZE, command(validConfig().build()).batchSize());
  }

  @Test
  void fallsBackToDefaultBatchSizeWhenNull() {
    assertEquals(
        InfluxDbSinkDto.DEFAULT_BATCH_SIZE,
        command(validConfig().batchSize(null).build()).batchSize());
  }

  @Test
  void failsFastWhenConfiguredCredentialCannotBeResolved() {
    InfluxDbSinkCommand command = command(validConfig().credentialId("missing-cred").build());
    assertThrows(
        RuntimeException.class,
        () -> command.initialize(new MetricClientProvider.NoopMetricClientProvider()));
    verifyNoInteractions(clientProvider);
  }
}
