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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.commands.databrickssink.AvroToDeltaSchemaConverter;
import io.fleak.zephflow.lib.commands.zerobussink.AvroToProtoDescriptorConverter;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaConfigurationDiagnosticsTest {
  private static final String PRIVATE_VALUE = "PRIVATE_SCHEMA_INPUT";

  @ParameterizedTest
  @MethodSource({"rootRejections", "nestedRejections"})
  void locatesExistingSchemaRejections(
      String converter, Map<String, Object> schema, ConfigurationPath path, String reason) {
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> convert(converter, schema));
    assertEquals(path, failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains(reason));
    assertFalse(failure.issues().toString().contains(PRIVATE_VALUE));
    assertEquals(failure.originalFailure().getMessage(), failure.getMessage());
    assertSame(failure.originalFailure(), failure.getCause());
  }

  static Stream<Arguments> rootRejections() {
    return Stream.of("delta", "protobuf")
        .flatMap(
            converter ->
                Stream.of(
                    Arguments.of(converter, Map.of("type", "string"), of("type"), "record"),
                    Arguments.of(
                        converter,
                        Map.of("type", "record", "name", PRIVATE_VALUE),
                        ConfigurationPath.root(),
                        "Invalid Avro schema"),
                    Arguments.of(
                        converter,
                        record(List.of("null", "string", "int")),
                        of("fields").index(0).field("type"),
                        "exactly one non-null"),
                    Arguments.of(
                        converter,
                        record("null"),
                        of("fields").index(0).field("type"),
                        converter.equals("delta") ? "Standalone null" : "Unsupported Avro type")));
  }

  static Stream<Arguments> nestedRejections() {
    return Stream.of("delta", "protobuf")
        .flatMap(
            converter ->
                Stream.of(
                    Arguments.of(
                        converter,
                        record(Map.of("type", "array", "items", List.of("string", "int"))),
                        of("fields").index(0).field("type").field("items"),
                        "exactly one non-null"),
                    Arguments.of(
                        converter,
                        record(Map.of("type", "map", "values", List.of("string", "int"))),
                        of("fields").index(0).field("type").field("values"),
                        "exactly one non-null"),
                    Arguments.of(
                        converter,
                        record(
                            List.of(
                                "null",
                                Map.of(
                                    "type",
                                    "record",
                                    "name",
                                    "Nested",
                                    "fields",
                                    List.of(
                                        Map.of(
                                            "name", "child", "type", List.of("string", "int")))))),
                        of("fields")
                            .index(0)
                            .field("type")
                            .index(1)
                            .field("fields")
                            .index(0)
                            .field("type"),
                        "exactly one non-null")));
  }

  @Test
  void locatesDeltaPrecisionAndKeepsBoundaryAcceptance() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> AvroToDeltaSchemaConverter.parse(record(decimal(39))));
    assertEquals(
        of("fields").index(0).field("type").field("precision"), failure.issues().getFirst().path());
    assertEquals(IllegalArgumentException.class, failure.originalFailure().getClass());
    assertTrue(failure.issues().getFirst().message().contains("between 0 and 38"));
    assertDoesNotThrow(() -> AvroToDeltaSchemaConverter.parse(record(decimal(38))));
  }

  @Test
  void locatesProtobufDecimalInNullableArrayAndMap() {
    assertProtobufFailure(
        record(decimal(10)), of("fields").index(0).field("type").field("logicalType"), "Decimal");
    assertProtobufFailure(
        record(Map.of("type", "array", "items", List.of("null", decimal(10)))),
        of("fields").index(0).field("type").field("items").index(1).field("logicalType"),
        "Decimal");
    assertProtobufFailure(
        record(Map.of("type", "map", "values", List.of("null", decimal(10)))),
        of("fields").index(0).field("type").field("values").index(1).field("logicalType"),
        "Decimal");
  }

  @Test
  void locatesUnsupportedNestedProtobufCollections() {
    assertProtobufFailure(
        record(Map.of("type", "array", "items", Map.of("type", "array", "items", "string"))),
        of("fields").index(0).field("type").field("items"),
        "Nested arrays or maps");
    assertProtobufFailure(
        record(Map.of("type", "map", "values", Map.of("type", "map", "values", "string"))),
        of("fields").index(0).field("type").field("values"),
        "Nested arrays or maps");
  }

  @Test
  void preservesNullableRecordsAndCollectionValues() {
    for (String converter : List.of("delta", "protobuf")) {
      for (Object type :
          List.of(
              "string",
              List.of("null", "int"),
              Map.of("type", "array", "items", List.of("null", "string")),
              Map.of("type", "map", "values", List.of("null", "long")),
              Map.of(
                  "type",
                  "record",
                  "name",
                  "Nested",
                  "fields",
                  List.of(Map.of("name", "child", "type", "boolean")))))
        assertDoesNotThrow(() -> convert(converter, record(type)));
    }
  }

  private static void assertProtobufFailure(
      Map<String, Object> schema, ConfigurationPath path, String reason) {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> AvroToProtoDescriptorConverter.toDescriptorProto(schema, "catalog.schema.table"));
    assertEquals(path, failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains(reason));
    assertEquals(IllegalArgumentException.class, failure.originalFailure().getClass());
  }

  private static void convert(String converter, Map<String, Object> schema) {
    if (converter.equals("delta")) AvroToDeltaSchemaConverter.parse(schema);
    else AvroToProtoDescriptorConverter.toDescriptorProto(schema, "catalog.schema.table");
  }

  private static Map<String, Object> record(Object type) {
    return Map.of(
        "type", "record", "name", "Root", "fields", List.of(Map.of("name", "value", "type", type)));
  }

  private static Map<String, Object> decimal(int precision) {
    return Map.of("type", "bytes", "logicalType", "decimal", "precision", precision, "scale", 0);
  }
}
