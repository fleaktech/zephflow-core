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
package io.fleak.zephflow.lib.commands.databrickssink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class DatabricksSinkCommandTest {

  private static final String CREDENTIAL_ID = "databricks-test-credential";

  private static final Map<String, Object> EMPLOYEES_AVRO_SCHEMA =
      Map.of(
          "type",
          "record",
          "name",
          "Employee",
          "fields",
          List.of(
              Map.of("name", "employee_id", "type", "int"),
              Map.of("name", "first_name", "type", "string"),
              Map.of("name", "last_name", "type", "string"),
              Map.of("name", "email", "type", List.of("null", "string")),
              Map.of("name", "hire_date", "type", Map.of("type", "int", "logicalType", "date")),
              Map.of("name", "salary", "type", "double"),
              Map.of("name", "is_active", "type", "boolean")));

  @Test
  @Disabled("Integration test - requires real Databricks environment with env vars set")
  void testWriteEmployeesToDatabricks() throws Exception {
    String host = System.getenv("DATABRICKS_HOST");
    String clientId = System.getenv("DATABRICKS_CLIENT_ID");
    String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");
    String warehouseId = System.getenv("DATABRICKS_WAREHOUSE_ID");

    assertNotNull(host, "DATABRICKS_HOST environment variable must be set");
    assertNotNull(clientId, "DATABRICKS_CLIENT_ID environment variable must be set");
    assertNotNull(clientSecret, "DATABRICKS_CLIENT_SECRET environment variable must be set");
    assertNotNull(warehouseId, "DATABRICKS_WAREHOUSE_ID environment variable must be set");

    DatabricksCredential credential =
        DatabricksCredential.builder()
            .host(host)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .build();

    JobContext jobContext =
        JobContext.builder()
            .otherProperties(Map.of(CREDENTIAL_ID, credential))
            .metricTags(Map.of("service", "test", "env", "integration"))
            .build();

    DatabricksSinkDto.Config config =
        DatabricksSinkDto.Config.builder()
            .databricksCredentialId(CREDENTIAL_ID)
            .volumePath("/Volumes/test/default/stg_landing_zone")
            .tableName("test.default.employees")
            .warehouseId(warehouseId)
            .avroSchema(EMPLOYEES_AVRO_SCHEMA)
            .batchSize(10)
            .flushIntervalMillis(5000)
            .cleanupAfterCopy(true)
            .build();

    DatabricksSinkCommand command =
        (DatabricksSinkCommand)
            new DatabricksSinkCommandFactory().createCommand("test-node", jobContext);

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    List<RecordFleakData> sourceEvents = createSampleEmployeeRecords();
    var context = command.getExecutionContext();

    // writeToSink buffers records; actual flush happens asynchronously or on terminate()
    SimpleSinkCommand.SinkResult result = command.writeToSink(sourceEvents, "test-user", context);
    assertTrue(result.getFailureEvents().isEmpty(), "No immediate errors should occur");

    // Wait for scheduled flush to complete (flushInterval=5000ms + COPY INTO execution time)
    Thread.sleep(30_000);

    // terminate() will flush any remaining buffered records
    command.terminate();

    // If we get here without exceptions, the data was written successfully
    // Check Databricks table for verification: SELECT * FROM test.default.employees
  }

  private static List<RecordFleakData> createSampleEmployeeRecords() {
    List<Map<String, Object>> employees = createSampleEmployees();
    List<RecordFleakData> records = new ArrayList<>();
    for (Map<String, Object> employee : employees) {
      records.add((RecordFleakData) FleakData.wrap(employee));
    }
    return records;
  }

  private static List<Map<String, Object>> createSampleEmployees() {
    return List.of(
        createEmployee(
            1001,
            "Alice",
            "Johnson",
            "alice.johnson@example.com",
            LocalDate.of(2023, 1, 15),
            75000.00,
            true),
        createEmployee(
            1002,
            "Bob",
            "Smith",
            "bob.smith@example.com",
            LocalDate.of(2023, 3, 20),
            82000.00,
            true),
        createEmployee(1003, "Carol", "Williams", null, LocalDate.of(2022, 11, 5), 95000.00, true),
        createEmployee(
            1004,
            "David",
            "Brown",
            "david.brown@example.com",
            LocalDate.of(2024, 2, 1),
            68000.00,
            false),
        createEmployee(
            1005,
            "Eva",
            "Martinez",
            "eva.martinez@example.com",
            LocalDate.of(2023, 7, 10),
            88500.50,
            true));
  }

  private static Map<String, Object> createEmployee(
      int employeeId,
      String firstName,
      String lastName,
      String email,
      LocalDate hireDate,
      double salary,
      boolean isActive) {
    Map<String, Object> employee = new HashMap<>();
    employee.put("employee_id", employeeId);
    employee.put("first_name", firstName);
    employee.put("last_name", lastName);
    employee.put("email", email);
    employee.put("hire_date", (int) hireDate.toEpochDay());
    employee.put("salary", salary);
    employee.put("is_active", isActive);
    return employee;
  }
}
