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

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupDatabricksCredential;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupDatabricksCredentialOpt;

import com.databricks.sdk.WorkspaceClient;
import io.delta.kernel.types.StructType;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSinkDto.Config;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

public class DatabricksSinkConfigValidator implements ConfigValidator {

  private static final Pattern VOLUME_PATH_PATTERN =
      Pattern.compile("^/Volumes/[^/]+/[^/]+/[^/]+.*$");
  private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[^.]+\\.[^.]+(?:\\.[^.]+)?$");

  private final Function<DatabricksCredential, WorkspaceClient> clientFactory;

  public DatabricksSinkConfigValidator() {
    this(DatabricksClientFactory::createClient);
  }

  DatabricksSinkConfigValidator(Function<DatabricksCredential, WorkspaceClient> clientFactory) {
    this.clientFactory = clientFactory;
  }

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    Config config = (Config) commandConfig;
    List<String> errors = new ArrayList<>();

    validateDatabricksCredential(config, jobContext, errors);
    validateVolumePath(config.getVolumePath(), errors);
    validateTableName(config.getTableName(), errors);
    validateWarehouseId(config.getWarehouseId(), errors);
    validateAvroSchema(config.getAvroSchema(), errors);
    validateBatchSize(config.getBatchSize(), errors);
    validateFlushInterval(config.getFlushIntervalMillis(), errors);

    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "Databricks sink configuration errors: " + String.join(", ", errors));
    }

    validateSchemaAgainstTable(config, jobContext);
  }

  private void validateSchemaAgainstTable(Config config, JobContext jobContext) {
    DatabricksCredential credential =
        lookupDatabricksCredential(jobContext, config.getDatabricksCredentialId());
    WorkspaceClient client = clientFactory.apply(credential);
    StructType schema = AvroToDeltaSchemaConverter.parse(config.getAvroSchema());
    new DeltaTableSchemaValidator(client).validateSchema(config.getTableName(), schema);
  }

  private void validateDatabricksCredential(
      Config config, JobContext jobContext, List<String> errors) {
    if (config.getDatabricksCredentialId() == null
        || config.getDatabricksCredentialId().trim().isEmpty()) {
      errors.add("databricksCredentialId is required");
      return;
    }

    var credentialOpt =
        lookupDatabricksCredentialOpt(jobContext, config.getDatabricksCredentialId());
    if (credentialOpt.isEmpty()) {
      errors.add(
          "databricksCredentialId '"
              + config.getDatabricksCredentialId()
              + "' was specified but no credential found in jobContext");
    }
  }

  private void validateVolumePath(String volumePath, List<String> errors) {
    if (volumePath == null || volumePath.trim().isEmpty()) {
      errors.add("volumePath is required");
      return;
    }

    if (!VOLUME_PATH_PATTERN.matcher(volumePath).matches()) {
      errors.add(
          "volumePath must match pattern: /Volumes/<catalog>/<schema>/<volume>/[path]. Got: "
              + volumePath);
    }
  }

  private void validateTableName(String tableName, List<String> errors) {
    if (tableName == null || tableName.trim().isEmpty()) {
      errors.add("tableName is required");
      return;
    }

    if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
      errors.add(
          "tableName must match pattern: <schema>.<table> or <catalog>.<schema>.<table>. Got: "
              + tableName);
    }
  }

  private void validateWarehouseId(String warehouseId, List<String> errors) {
    if (warehouseId == null || warehouseId.trim().isEmpty()) {
      errors.add("warehouseId is required");
    }
  }

  private void validateBatchSize(int batchSize, List<String> errors) {
    if (batchSize <= 0) {
      errors.add("batchSize must be positive");
    } else if (batchSize > 100000) {
      errors.add("batchSize should not exceed 100,000 for optimal performance");
    }
  }

  private void validateFlushInterval(long flushIntervalMillis, List<String> errors) {
    if (flushIntervalMillis <= 0) {
      errors.add("flushIntervalMillis must be positive");
    } else if (flushIntervalMillis < 1000) {
      errors.add("flushIntervalMillis should be at least 1000ms (1 second)");
    }
  }

  private void validateAvroSchema(Map<String, Object> avroSchema, List<String> errors) {
    if (avroSchema == null || avroSchema.isEmpty()) {
      errors.add("avroSchema is required");
      return;
    }

    try {
      AvroToDeltaSchemaConverter.parse(avroSchema);
    } catch (Exception e) {
      errors.add("avroSchema is invalid: " + e.getMessage());
    }
  }
}
