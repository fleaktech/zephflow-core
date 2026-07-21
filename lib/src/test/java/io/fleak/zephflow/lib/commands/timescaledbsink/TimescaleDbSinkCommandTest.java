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
package io.fleak.zephflow.lib.commands.timescaledbsink;

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

class TimescaleDbSinkCommandTest {

  private JobContext jobContext;
  private final TimescaleHypertableInitializer initializer =
      mock(TimescaleHypertableInitializer.class);

  @BeforeEach
  void setUp() {
    jobContext = new JobContext();
    jobContext.setMetricTags(TestUtils.JOB_CONTEXT.getMetricTags());
    jobContext.setOtherProperties(Map.of());
  }

  private TimescaleDbSinkCommand command(TimescaleDbSinkDto.Config config) {
    TimescaleDbSinkCommand command =
        (TimescaleDbSinkCommand)
            new TimescaleDbSinkCommandFactory(initializer).createCommand("node", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    return command;
  }

  private static TimescaleDbSinkDto.Config.ConfigBuilder validConfig() {
    return TimescaleDbSinkDto.Config.builder()
        .jdbcUrl("jdbc:postgresql://localhost:5432/tsdb")
        .tableName("metrics")
        .timeColumn("ts");
  }

  @Test
  void reportsCommandName() {
    assertEquals("timescaledbsink", command(validConfig().build()).commandName());
  }

  @Test
  void usesConfiguredBatchSize() {
    assertEquals(250, command(validConfig().batchSize(250).build()).batchSize());
  }

  @Test
  void createsHypertableOnInitializeByDefault() {
    command(validConfig().build()).initialize(new MetricClientProvider.NoopMetricClientProvider());
    verify(initializer)
        .ensureHypertable(
            eq("jdbc:postgresql://localhost:5432/tsdb"), any(), any(), eq("\"metrics\""), eq("ts"));
  }

  @Test
  void skipsHypertableCreationWhenDisabled() {
    command(validConfig().createHypertable(false).build())
        .initialize(new MetricClientProvider.NoopMetricClientProvider());
    verifyNoInteractions(initializer);
  }

  @Test
  void failsFastWhenConfiguredCredentialCannotBeResolved() {
    TimescaleDbSinkCommand command = command(validConfig().credentialId("missing-cred").build());
    assertThrows(
        RuntimeException.class,
        () -> command.initialize(new MetricClientProvider.NoopMetricClientProvider()));
  }
}
