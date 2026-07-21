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

import com.fasterxml.jackson.core.type.TypeReference;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class InfluxDbSinkCommandIntegrationTest {

  private static final String ORG = "test-org";
  private static final String BUCKET = "test-bucket";
  private static final String TOKEN = "my-test-token";
  private static final String CRED_ID = "influx_token";

  @Container
  static final GenericContainer<?> influxDb =
      new GenericContainer<>(DockerImageName.parse("influxdb:2.7"))
          .withExposedPorts(8086)
          .withEnv("DOCKER_INFLUXDB_INIT_MODE", "setup")
          .withEnv("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
          .withEnv("DOCKER_INFLUXDB_INIT_PASSWORD", "password123")
          .withEnv("DOCKER_INFLUXDB_INIT_ORG", ORG)
          .withEnv("DOCKER_INFLUXDB_INIT_BUCKET", BUCKET)
          .withEnv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", TOKEN)
          .waitingFor(Wait.forHttp("/health").forStatusCode(200));

  private static String url() {
    return "http://" + influxDb.getHost() + ":" + influxDb.getMappedPort(8086);
  }

  private static RecordFleakData weather(String city, double temp, long humidity, long ts) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("city", city);
    m.put("temp", temp);
    m.put("humidity", humidity);
    m.put("ts", ts);
    return (RecordFleakData) FleakData.wrap(m);
  }

  @Test
  void writesMappedPointsThatCanBeQueriedBack() {
    JobContext jobContext = new JobContext();
    jobContext.setMetricTags(TestUtils.JOB_CONTEXT.getMetricTags());
    jobContext.setOtherProperties(Map.of(CRED_ID, new ApiKeyCredential(TOKEN)));

    List<RecordFleakData> events =
        List.of(
            weather("SF", 18.5, 60, 1_700_000_000_000L),
            weather("NYC", 5.0, 40, 1_700_000_001_000L));

    InfluxDbSinkCommand command =
        (InfluxDbSinkCommand)
            new InfluxDbSinkCommandFactory().createCommand("test-node", jobContext);

    InfluxDbSinkDto.Config config =
        InfluxDbSinkDto.Config.builder()
            .url(url())
            .org(ORG)
            .bucket(BUCKET)
            .credentialId(CRED_ID)
            .measurement("weather")
            .tagFields(List.of("city"))
            .fieldFields(List.of("temp", "humidity"))
            .timestampField("ts")
            .build();

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = command.getExecutionContext();

    var result = command.writeToSink(events, "test_user", context);

    assertEquals(2, result.getSuccessCount());
    assertEquals(0, result.getFailureEvents().size());

    // Query the temperature field back and confirm both points landed with their tags/values.
    List<FluxRecord> temps = queryTemps();
    assertEquals(2, temps.size());

    Map<String, Double> byCity = new LinkedHashMap<>();
    for (FluxRecord r : temps) {
      byCity.put((String) r.getValueByKey("city"), (Double) r.getValue());
    }
    assertEquals(18.5, byCity.get("SF"));
    assertEquals(5.0, byCity.get("NYC"));
  }

  private List<FluxRecord> queryTemps() {
    String flux =
        String.format(
            "from(bucket: \"%s\") |> range(start: 0) "
                + "|> filter(fn: (r) => r._measurement == \"weather\" and r._field == \"temp\")",
            BUCKET);
    try (InfluxDBClient client = new InfluxDbClientProvider().create(url(), TOKEN, ORG, BUCKET)) {
      List<FluxRecord> records = new ArrayList<>();
      for (FluxTable table : client.getQueryApi().query(flux, ORG)) {
        records.addAll(table.getRecords());
      }
      return records;
    }
  }
}
