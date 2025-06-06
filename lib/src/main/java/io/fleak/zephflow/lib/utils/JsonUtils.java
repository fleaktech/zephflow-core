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
package io.fleak.zephflow.lib.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fleak.zephflow.api.structure.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public abstract class JsonUtils {
  public static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    JavaTimeModule javaTimeModule = new JavaTimeModule();
    OBJECT_MAPPER.registerModule(javaTimeModule);
    OBJECT_MAPPER.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    // Register custom serializers/deserializers for FleakData types
    SimpleModule fleakDataModule = new SimpleModule("FleakDataModule");
    fleakDataModule.addSerializer(FleakData.class, new FleakDataSerializer());
    fleakDataModule.addDeserializer(FleakData.class, new FleakDataDeserializer());
    OBJECT_MAPPER.registerModule(fleakDataModule);
  }

  /**
   * Converts an object to a JSON string. For FleakData objects, this will use the custom
   * serialization logic.
   */
  public static String toJsonString(Object object) {
    if (Objects.isNull(object)) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts a JSON string to an object of the specified type. */
  public static <T> T fromJsonString(String jsonStr, TypeReference<T> typeReference) {
    if (Objects.isNull(jsonStr)) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(jsonStr, typeReference);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clz) {
    if (Objects.isNull(jsonStr)) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(jsonStr, clz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromJsonResource(String resourcePath, TypeReference<T> typeReference)
      throws IOException {
    try (InputStream in = JsonUtils.class.getResourceAsStream(resourcePath)) {
      return fromJsonInputStream(in, typeReference);
    }
  }

  public static <T> T fromJsonInputStream(InputStream in, TypeReference<T> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(in, typeReference);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromJsonBytes(byte[] bytes, TypeReference<T> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(bytes, typeReference);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static JsonNode convertToJsonNode(Object object) {
    if (Objects.isNull(object)) {
      return null;
    }
    return OBJECT_MAPPER.convertValue(object, JsonNode.class);
  }

  /**
   * Converts an object to FleakData. Now leverages Jackson for consistent
   * serialization/deserialization.
   */
  public static FleakData fromObject(Object object) {
    if (object == null) {
      return null;
    }

    if (object instanceof FleakData) {
      return (FleakData) object;
    }

    // Convert to JSON and back for consistent handling
    JsonNode jsonNode = convertToJsonNode(object);
    return fromJsonNode(jsonNode);
  }

  public static RecordFleakData fromJsonPayload(ObjectNode jsonObj) {
    HashMap<String, FleakData> payload = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> it = jsonObj.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> e = it.next();
      String k = e.getKey();
      JsonNode v = e.getValue();
      FleakData data = fromJsonNode(v);
      payload.put(k, data);
    }
    return new RecordFleakData(payload);
  }

  public static FleakData fromJsonNode(JsonNode node) {
    if (node == null) {
      return null;
    }

    JsonNodeType nodeType = node.getNodeType();
    return switch (nodeType) {
      case ARRAY -> fromJsonArray((ArrayNode) node);
      case BOOLEAN -> new BooleanPrimitiveFleakData(node.booleanValue());
      case NUMBER -> fromJsonNumber((NumericNode) node);
      case OBJECT -> fromJsonPayload((ObjectNode) node);
      case STRING -> new StringPrimitiveFleakData(node.textValue());
      case NULL -> null;
      default ->
          throw new RuntimeException(
              String.format("value (%s) is unsupported Json Type (%s) ", node, nodeType));
    };
  }

  public static NumberPrimitiveFleakData fromJsonNumber(NumericNode numericNode) {
    double numberValue = numericNode.numberValue().doubleValue();
    NumberPrimitiveFleakData.NumberType numberType =
        switch (numericNode.numberType()) {
          case INT -> NumberPrimitiveFleakData.NumberType.INT;
          case LONG -> NumberPrimitiveFleakData.NumberType.LONG;
          case FLOAT -> NumberPrimitiveFleakData.NumberType.FLOAT;
          case DOUBLE -> NumberPrimitiveFleakData.NumberType.DOUBLE;
          default ->
              throw new RuntimeException(
                  String.format(
                      "value (%s) is unsupported Json number Type (%s) ",
                      numberValue, numericNode.numberType()));
        };
    return new NumberPrimitiveFleakData(numberValue, numberType);
  }

  public static ArrayFleakData fromJsonArray(ArrayNode arrayNode) {
    List<FleakData> list = new ArrayList<>();
    for (JsonNode element : arrayNode) {
      FleakData d = fromJsonNode(element);
      list.add(d);
    }
    return new ArrayFleakData(list);
  }

  public static ObjectNode toJsonPayload(RecordFleakData event) {
    ObjectNode objectNode = JsonUtils.OBJECT_MAPPER.createObjectNode();
    Map<String, FleakData> payload = event.getPayload();
    payload.forEach(
        (k, v) -> {
          JsonNode jsonNode = toJsonNode(v);
          objectNode.set(k, jsonNode);
        });
    return objectNode;
  }

  public static JsonNode toJsonNode(FleakData data) {
    if (data == null) {
      return null;
    }

    if (data instanceof RecordFleakData recordFleakData) {
      return toJsonPayload(recordFleakData);
    }

    if (data instanceof ArrayFleakData arrayFleakData) {
      return toArrayNode(arrayFleakData);
    }

    if (data instanceof StringPrimitiveFleakData) {
      return new TextNode(data.getStringValue());
    }

    if (data instanceof BooleanPrimitiveFleakData) {
      return BooleanNode.valueOf(data.isTrueValue());
    }

    if (data instanceof NumberPrimitiveFleakData numberPrimitiveFleakData) {
      return toJsonNumber(numberPrimitiveFleakData);
    }

    throw new RuntimeException(String.format("value (%s) is unsupported fleak data type", data));
  }

  static NumericNode toJsonNumber(NumberPrimitiveFleakData data) {
    double value = data.getNumberValue();
    return switch (data.getNumberType()) {
      case INT -> IntNode.valueOf((int) value);
      case LONG -> LongNode.valueOf((long) value);
      case FLOAT -> FloatNode.valueOf((float) value);
      case DOUBLE -> DoubleNode.valueOf(value);
      default -> throw new RuntimeException("value is not supported number type: " + data);
    };
  }

  /**
   * Convert a FleakData array to a Jackson ArrayNode. Uses the new serialization mechanism for
   * consistency.
   */
  public static ArrayNode toArrayNode(ArrayFleakData data) {
    ArrayNode arrayNode = JsonUtils.OBJECT_MAPPER.createArrayNode();
    data.getArrayPayload()
        .forEach(
            fleakData -> {
              JsonNode node = toJsonNode(fleakData);
              arrayNode.add(node);
            });
    return arrayNode;
  }

  /** Load FleakData from a JSON string. Uses the Jackson deserializer for consistency. */
  public static FleakData loadFleakDataFromJsonString(String jsonStr) {
    if (jsonStr == null) {
      return null;
    }

    try {
      // This will use the registered FleakDataDeserializer
      JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonStr);
      return fromJsonNode(jsonNode);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse JSON string: " + jsonStr, e);
    }
  }

  public static FleakData loadFleakDataFromJsonResource(String resourcePath) throws IOException {
    try (InputStream in = JsonUtils.class.getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      JsonNode jsonNode = OBJECT_MAPPER.readTree(in);
      return fromJsonNode(jsonNode);
    }
  }
}
