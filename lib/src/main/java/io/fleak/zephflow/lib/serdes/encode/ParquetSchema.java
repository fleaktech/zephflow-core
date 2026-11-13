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
package io.fleak.zephflow.lib.serdes.encode;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.at;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.commands.databrickssink.AvroToDeltaSchemaConverter;
import java.util.Map;

/** Reads a Parquet writer schema while preserving its input location in diagnostics. */
final class ParquetSchema {
  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

  private ParquetSchema() {}

  static Map<String, Object> parse(String sink, JsonNode schema) {
    Map<String, Object> avroSchema = read(sink, schema);
    try {
      AvroToDeltaSchemaConverter.parse(avroSchema);
    } catch (ConfigurationValidationException failure) {
      var schemaPath = of("format", "schema");
      throw new ConfigurationValidationException(
          new IllegalArgumentException(
              sink
                  + ": parquet option schema is not a valid avro record schema: "
                  + failure.getMessage(),
              failure.originalFailure()),
          failure.issues().stream()
              .map(
                  issue ->
                      new ConfigurationIssue(
                          schema.isObject() ? schemaPath.append(issue.path()) : schemaPath,
                          issue.message()))
              .toList());
    } catch (RuntimeException e) {
      throw at(
          of("format", "schema"),
          new IllegalArgumentException(
              sink + ": parquet option schema is not a valid avro record schema: " + e.getMessage(),
              e),
          "Parquet schema must be a valid Avro record schema.");
    }
    return avroSchema;
  }

  private static Map<String, Object> read(String sink, JsonNode schema) {
    if (schema == null || schema.isNull()) {
      throw at(
          of("format", "schema"),
          new IllegalArgumentException(sink + ": parquet option schema is required when writing"),
          "An Avro record schema is required when writing Parquet.");
    }
    JsonNode node = schema;
    if (schema.isTextual()) {
      try {
        node = OBJECT_MAPPER.readTree(schema.asText());
      } catch (JsonProcessingException e) {
        node = null;
      }
    }
    if (node == null || !node.isObject()) {
      throw at(
          of("format", "schema"),
          new IllegalArgumentException(
              sink
                  + ": parquet option schema must be an avro record schema, as a JSON object or a"
                  + " string holding one"),
          "Expected an Avro record schema as a JSON object or a string containing one.");
    }
    return OBJECT_MAPPER.convertValue(node, MAP);
  }
}
