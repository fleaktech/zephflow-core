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

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class TimescaleDbSinkCommandIntegrationTest {

  private static final String CRED_ID = "tsdb_cred";

  @Container
  static final PostgreSQLContainer<?> timescale =
      new PostgreSQLContainer<>(
          DockerImageName.parse("timescale/timescaledb:2.17.2-pg16")
              .asCompatibleSubstituteFor("postgres"));

  private static RecordFleakData reading(long ts, String host, double val) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ts", ts);
    m.put("host", host);
    m.put("val", val);
    return (RecordFleakData) FleakData.wrap(m);
  }

  @SneakyThrows
  private static Connection connect() {
    return DriverManager.getConnection(
        timescale.getJdbcUrl(), timescale.getUsername(), timescale.getPassword());
  }

  @Test
  @SneakyThrows
  void createsHypertableAndWritesRows() {
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, host TEXT, val DOUBLE PRECISION)");
    }

    JobContext jobContext = new JobContext();
    jobContext.setMetricTags(TestUtils.JOB_CONTEXT.getMetricTags());
    jobContext.setOtherProperties(
        Map.of(
            CRED_ID,
            new UsernamePasswordCredential(timescale.getUsername(), timescale.getPassword())));

    List<RecordFleakData> events =
        List.of(reading(1_700_000_000_000L, "a", 18.5), reading(1_700_000_001_000L, "b", 5.0));

    TimescaleDbSinkCommand command =
        (TimescaleDbSinkCommand)
            new TimescaleDbSinkCommandFactory().createCommand("node", jobContext);

    TimescaleDbSinkDto.Config config =
        TimescaleDbSinkDto.Config.builder()
            .jdbcUrl(timescale.getJdbcUrl())
            .credentialId(CRED_ID)
            .tableName("metrics")
            .timeColumn("ts")
            .build();

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var result = command.writeToSink(events, "user", command.getExecutionContext());

    assertEquals(2, result.getSuccessCount());
    assertEquals(0, result.getFailureEvents().size());
    assertTrue(isHypertable("metrics"));
    assertEquals(2, rowCount());
  }

  @SneakyThrows
  private boolean isHypertable(String table) {
    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT 1 FROM timescaledb_information.hypertables"
                    + " WHERE hypertable_name = '"
                    + table
                    + "'")) {
      return rs.next();
    }
  }

  @SneakyThrows
  private int rowCount() {
    try (Connection connection = connect();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM metrics")) {
      rs.next();
      return rs.getInt(1);
    }
  }
}
