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
package io.fleak.zephflow.lib.commands.zerobussink;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupDatabricksCredentialOpt;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.zerobussink.ZerobusSinkDto.Config;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ZerobusSinkConfigValidator implements ConfigValidator {

  // Fully-qualified Unity Catalog name is checked as three non-blank dot-separated parts in
  // validateTableName. NOTE: backtick-quoted identifiers that themselves contain dots are not
  // supported by this simple three-part check.

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    Config config = (Config) commandConfig;
    List<String> errors = new ArrayList<>();

    validateDatabricksCredential(config, jobContext, errors);
    validateZerobusEndpoint(config.getZerobusEndpoint(), errors);
    validateTableName(config.getTableName(), errors);
    validateEncodingType(config.getEncodingType(), errors);

    // avroSchema is only used to build the protobuf descriptor. In JSON mode the stream is created
    // without a descriptor (sdk.createJsonStream), so a schema is neither required nor used.
    if (ZerobusSinkDto.ENCODING_PROTOBUF.equalsIgnoreCase(config.getEncodingType())) {
      Map<String, Object> avroSchema = config.getAvroSchema();
      if (validateAvroSchema(avroSchema, errors)) {
        try {
          AvroToProtoDescriptorConverter.toDescriptorProto(avroSchema, config.getTableName());
        } catch (Exception e) {
          errors.add("avroSchema cannot be converted to a protobuf descriptor: " + e.getMessage());
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "Zerobus sink configuration errors: " + String.join(", ", errors));
    }
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

  private void validateZerobusEndpoint(String zerobusEndpoint, List<String> errors) {
    if (zerobusEndpoint == null || zerobusEndpoint.trim().isEmpty()) {
      errors.add("zerobusEndpoint is required");
      return;
    }
    // The flusher passes the raw config value straight to the SDK, so validate exactly what the SDK
    // will receive. Reject surrounding whitespace rather than silently trimming here (which would
    // validate one string while the SDK gets another).
    if (!zerobusEndpoint.equals(zerobusEndpoint.trim())) {
      errors.add(
          "zerobusEndpoint must not have leading/trailing whitespace. Got: " + zerobusEndpoint);
      return;
    }
    // Validate it's a well-formed https URL. We deliberately do NOT constrain the host shape
    // (e.g. requiring a ".zerobus." segment): private-link and custom-DNS endpoints are valid.
    try {
      URI uri = URI.create(zerobusEndpoint);
      if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null) {
        errors.add("zerobusEndpoint must be an https URL. Got: " + zerobusEndpoint);
      }
    } catch (IllegalArgumentException e) {
      errors.add("zerobusEndpoint is not a valid URL: " + zerobusEndpoint);
    }
  }

  private void validateTableName(String tableName, List<String> errors) {
    if (tableName == null || tableName.trim().isEmpty()) {
      errors.add("tableName is required");
      return;
    }
    // Same as the endpoint: the raw value reaches createProtoStream, so reject untrimmed input.
    if (!tableName.equals(tableName.trim())) {
      errors.add("tableName must not have leading/trailing whitespace. Got: " + tableName);
      return;
    }
    // Three dot-separated parts, none blank/whitespace-only. TABLE_NAME_PATTERN's [^.]+ would admit
    // a blank part like "catalog. .table", so also reject any whitespace-only segment.
    String[] parts = tableName.split("\\.", -1);
    if (parts.length != 3 || Arrays.stream(parts).anyMatch(p -> p.trim().isEmpty())) {
      errors.add("tableName must match pattern: <catalog>.<schema>.<table>. Got: " + tableName);
    }
  }

  private void validateEncodingType(String encodingType, List<String> errors) {
    if (encodingType == null || encodingType.trim().isEmpty()) {
      errors.add("encodingType is required");
      return;
    }
    if (!ZerobusSinkDto.ENCODING_PROTOBUF.equalsIgnoreCase(encodingType)
        && !ZerobusSinkDto.ENCODING_JSON.equalsIgnoreCase(encodingType)) {
      errors.add(
          "encodingType must be '"
              + ZerobusSinkDto.ENCODING_PROTOBUF
              + "' or '"
              + ZerobusSinkDto.ENCODING_JSON
              + "'. Got: "
              + encodingType);
    }
  }

  private boolean validateAvroSchema(Map<String, Object> avroSchema, List<String> errors) {
    if (avroSchema == null || avroSchema.isEmpty()) {
      errors.add("avroSchema is required");
      return false;
    }
    try {
      AvroToProtoDescriptorConverter.parseAvro(avroSchema);
      return true;
    } catch (Exception e) {
      errors.add("avroSchema is invalid: " + e.getMessage());
      return false;
    }
  }
}
