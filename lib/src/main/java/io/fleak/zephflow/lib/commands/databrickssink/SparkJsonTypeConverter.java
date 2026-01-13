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

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.types.*;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts Spark SQL JSON type representation (from Databricks typeJson) to Delta Kernel DataType.
 *
 * <p>typeJson format examples:
 *
 * <ul>
 *   <li>Simple: "integer", "string", "boolean"
 *   <li>Decimal: "decimal(10,2)"
 *   <li>Array: {"type":"array","elementType":"string","containsNull":true}
 *   <li>Map: {"type":"map","keyType":"string","valueType":"integer","valueContainsNull":true}
 *   <li>Struct: {"type":"struct","fields":[{"name":"city","type":"string","nullable":true}]}
 * </ul>
 */
public class SparkJsonTypeConverter {

  private static final Pattern DECIMAL_PATTERN =
      Pattern.compile("^decimal\\((\\d+),\\s*(\\d+)\\)$", Pattern.CASE_INSENSITIVE);

  public static DataType convert(String typeJson) {
    if (typeJson == null || typeJson.isBlank()) {
      throw new IllegalArgumentException("typeJson cannot be null or blank");
    }

    String trimmed = typeJson.trim();

    if (trimmed.startsWith("{")) {
      return convertComplexType(trimmed);
    } else {
      return convertSimpleType(trimmed);
    }
  }

  private static DataType convertSimpleType(String typeName) {
    String normalized = typeName.toLowerCase().trim();

    return switch (normalized) {
      case "string", "varchar", "char" -> StringType.STRING;
      case "integer", "int" -> IntegerType.INTEGER;
      case "long", "bigint" -> LongType.LONG;
      case "short", "smallint" -> ShortType.SHORT;
      case "byte", "tinyint" -> ByteType.BYTE;
      case "float" -> FloatType.FLOAT;
      case "double" -> DoubleType.DOUBLE;
      case "boolean" -> BooleanType.BOOLEAN;
      case "binary" -> BinaryType.BINARY;
      case "date" -> DateType.DATE;
      case "timestamp" -> TimestampType.TIMESTAMP;
      case "timestamp_ntz" -> TimestampNTZType.TIMESTAMP_NTZ;
      default -> {
        Matcher decimalMatcher = DECIMAL_PATTERN.matcher(normalized);
        if (decimalMatcher.matches()) {
          int precision = Integer.parseInt(decimalMatcher.group(1));
          int scale = Integer.parseInt(decimalMatcher.group(2));
          yield new DecimalType(precision, scale);
        }
        throw new IllegalArgumentException("Unknown Spark type: " + typeName);
      }
    };
  }

  private static DataType convertComplexType(String typeJson) {
    JsonNode node = JsonUtils.OBJECT_MAPPER.valueToTree(parseJson(typeJson));
    return convertJsonNode(node);
  }

  private static Object parseJson(String json) {
    try {
      return JsonUtils.OBJECT_MAPPER.readValue(json, Object.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON: " + json, e);
    }
  }

  private static DataType convertJsonNode(JsonNode node) {
    if (node.isTextual()) {
      return convertSimpleType(node.asText());
    }

    if (!node.isObject()) {
      throw new IllegalArgumentException("Expected object or string, got: " + node.getNodeType());
    }

    JsonNode typeNode = node.path("type");

    if (typeNode.isObject()) {
      return convertJsonNode(typeNode);
    }

    String type = typeNode.asText();

    return switch (type) {
      case "array" -> convertArrayType(node);
      case "map" -> convertMapType(node);
      case "struct" -> convertStructType(node);
      case "decimal" -> {
        int precision = node.path("precision").asInt(10);
        int scale = node.path("scale").asInt(0);
        yield new DecimalType(precision, scale);
      }
      case "udt" -> {
        JsonNode sqlTypeNode = node.path("sqlType");
        if (sqlTypeNode.isMissingNode()) {
          throw new IllegalArgumentException("UDT type missing 'sqlType' field");
        }
        yield convertJsonNode(sqlTypeNode);
      }
      default -> convertSimpleType(type);
    };
  }

  private static ArrayType convertArrayType(JsonNode node) {
    JsonNode elementTypeNode = node.path("elementType");
    boolean containsNull = node.path("containsNull").asBoolean(true);

    DataType elementType;
    if (elementTypeNode.isTextual()) {
      elementType = convertSimpleType(elementTypeNode.asText());
    } else {
      elementType = convertJsonNode(elementTypeNode);
    }

    return new ArrayType(elementType, containsNull);
  }

  private static MapType convertMapType(JsonNode node) {
    JsonNode keyTypeNode = node.path("keyType");
    JsonNode valueTypeNode = node.path("valueType");
    boolean valueContainsNull = node.path("valueContainsNull").asBoolean(true);

    DataType keyType;
    if (keyTypeNode.isTextual()) {
      keyType = convertSimpleType(keyTypeNode.asText());
    } else {
      keyType = convertJsonNode(keyTypeNode);
    }

    DataType valueType;
    if (valueTypeNode.isTextual()) {
      valueType = convertSimpleType(valueTypeNode.asText());
    } else {
      valueType = convertJsonNode(valueTypeNode);
    }

    return new MapType(keyType, valueType, valueContainsNull);
  }

  private static StructType convertStructType(JsonNode node) {
    JsonNode fieldsNode = node.path("fields");
    if (!fieldsNode.isArray()) {
      throw new IllegalArgumentException("Struct type must have 'fields' array");
    }

    List<StructField> fields = new ArrayList<>();
    for (JsonNode fieldNode : fieldsNode) {
      String name = fieldNode.path("name").asText();
      boolean nullable = fieldNode.path("nullable").asBoolean(true);

      JsonNode typeNode = fieldNode.path("type");
      DataType dataType;
      if (typeNode.isTextual()) {
        dataType = convertSimpleType(typeNode.asText());
      } else {
        dataType = convertJsonNode(typeNode);
      }

      fields.add(new StructField(name, dataType, nullable));
    }

    return new StructType(fields);
  }

  public static StructType buildStructTypeFromColumns(
      java.util.Collection<com.databricks.sdk.service.catalog.ColumnInfo> columns) {
    List<StructField> fields = new ArrayList<>();
    for (var column : columns) {
      String typeJson = column.getTypeJson();
      if (typeJson == null || typeJson.isBlank()) {
        throw new IllegalArgumentException(
            "Column '" + column.getName() + "' has no typeJson. Cannot validate schema.");
      }

      String name = column.getName();
      Boolean nullableObj = column.getNullable();
      boolean nullable = nullableObj == null || nullableObj;

      DataType dataType = convert(typeJson);

      fields.add(new StructField(name, dataType, nullable));
    }
    return new StructType(fields);
  }
}
