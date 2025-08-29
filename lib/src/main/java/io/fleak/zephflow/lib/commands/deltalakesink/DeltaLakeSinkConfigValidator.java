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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeSinkDto.Config;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DeltaLakeSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    Config config = (Config) commandConfig;
    List<String> errors = new ArrayList<>();

    // Validate credentials if credentialId is specified
    if (config.getCredentialId() != null) {
      var credentialOpt = lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
      if (credentialOpt.isEmpty()) {
        errors.add(
            "credentialId '"
                + config.getCredentialId()
                + "' was specified but no credential found in jobContext");
      }
    }

    validateTablePath(config.getTablePath(), errors);
    validateBatchSize(config.getBatchSize(), errors);
    validatePartitionColumns(config.getPartitionColumns(), errors);
    validateHadoopConfiguration(config.getHadoopConfiguration(), errors);

    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "Delta Lake sink configuration errors: " + String.join(", ", errors));
    }
  }

  private void validateTablePath(String tablePath, List<String> errors) {
    if (tablePath == null || tablePath.trim().isEmpty()) {
      errors.add("tablePath is required");
      return;
    }

    try {
      URI uri = new URI(tablePath);
      String scheme = uri.getScheme();

      if (scheme == null) {
        // Local path - validate it's a valid path
        try {
          Paths.get(tablePath);
        } catch (Exception e) {
          errors.add("Invalid local path: " + tablePath);
        }
      } else if (!isValidScheme(scheme)) {
        errors.add(
            "Unsupported scheme: " + scheme + ". Supported schemes: file, s3, s3a, hdfs, abfs, gs");
      }
    } catch (URISyntaxException e) {
      errors.add("Invalid URI format for tablePath: " + tablePath);
    }
  }

  private boolean isValidScheme(String scheme) {
    return "file".equals(scheme)
        || "s3".equals(scheme)
        || "s3a".equals(scheme)
        || "hdfs".equals(scheme)
        || "abfs".equals(scheme)
        || "gs".equals(scheme);
  }

  private void validateBatchSize(int batchSize, List<String> errors) {
    if (batchSize <= 0) {
      errors.add("batchSize must be positive");
    } else if (batchSize > 10000) {
      errors.add("batchSize should not exceed 10,000 for optimal performance");
    }
  }

  private void validatePartitionColumns(List<String> partitionColumns, List<String> errors) {
    if (partitionColumns != null) {
      for (String column : partitionColumns) {
        if (column == null || column.trim().isEmpty()) {
          errors.add("Partition column names cannot be null or empty");
          break;
        }
      }
    }
  }

  private void validateHadoopConfiguration(
      java.util.Map<String, String> hadoopConfig, List<String> errors) {
    if (hadoopConfig != null) {
      for (java.util.Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        String key = entry.getKey();
        if (key == null || key.trim().isEmpty()) {
          errors.add("Hadoop configuration keys cannot be null or empty");
          break;
        }

        // Warn about cloud storage credentials in Hadoop config - should use credentialId instead
        if (key.equals("fs.s3a.access.key") || key.equals("fs.s3a.secret.key")) {
          errors.add(
              "S3 credentials (fs.s3a.access.key, fs.s3a.secret.key) should not be set in "
                  + "hadoopConfiguration. Use 'credentialId' field instead to reference stored credentials");
        } else if (key.startsWith("fs.azure.account.key.")) {
          errors.add(
              "Azure storage account keys should not be set in hadoopConfiguration. "
                  + "Use 'credentialId' field instead to reference stored credentials");
        } else if (key.equals("fs.gs.auth.service.account.email") || key.contains("google.cloud")) {
          errors.add(
              "GCS credentials should not be set in hadoopConfiguration. "
                  + "Use 'credentialId' field instead to reference stored credentials");
        }
      }
    }
  }
}
