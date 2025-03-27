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
package io.fleak.zephflow.api.structure;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom deserializer for FleakData objects. Handles deserialization based on the JSON value type.
 */
public class FleakDataDeserializer extends StdDeserializer<FleakData> {

  public FleakDataDeserializer() {
    this(null);
  }

  protected FleakDataDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public FleakData deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    // Instead of using readTree, we'll handle the JSON tokens directly
    // This approach avoids any issues with FAIL_ON_TRAILING_TOKENS
    return deserializeToken(p, p.currentToken());
  }

  private FleakData deserializeToken(JsonParser p, JsonToken token) throws IOException {
    if (token == null) {
      token = p.nextToken();
    }

    if (token == null || token == JsonToken.VALUE_NULL) {
      return null;
    } else if (token == JsonToken.VALUE_STRING) {
      return new StringPrimitiveFleakData(p.getText());
    } else if (token == JsonToken.VALUE_NUMBER_INT || token == JsonToken.VALUE_NUMBER_FLOAT) {
      return deserializeNumber(p);
    } else if (token == JsonToken.VALUE_TRUE || token == JsonToken.VALUE_FALSE) {
      return new BooleanPrimitiveFleakData(p.getBooleanValue());
    } else if (token == JsonToken.START_OBJECT) {
      return deserializeObject(p);
    } else if (token == JsonToken.START_ARRAY) {
      return deserializeArray(p);
    } else {
      throw new IOException("Unsupported JSON token: " + token);
    }
  }

  private NumberPrimitiveFleakData deserializeNumber(JsonParser p) throws IOException {
    NumberPrimitiveFleakData.NumberType type;
    double value;

    if (p.isNaN()) {
      value = Double.NaN;
      type = NumberPrimitiveFleakData.NumberType.DOUBLE;
    } else if (p.getNumberType() != null) {
      type =
          switch (p.getNumberType()) {
            case INT -> {
              value = p.getIntValue();
              yield NumberPrimitiveFleakData.NumberType.INT;
            }
            case LONG -> {
              value = p.getLongValue();
              yield NumberPrimitiveFleakData.NumberType.LONG;
            }
            case FLOAT -> {
              value = p.getFloatValue();
              yield NumberPrimitiveFleakData.NumberType.FLOAT;
            }
            default -> {
              value = p.getDoubleValue();
              yield NumberPrimitiveFleakData.NumberType.DOUBLE;
            }
          };
    } else {
      value = p.getDoubleValue();
      type = NumberPrimitiveFleakData.NumberType.DOUBLE;
    }

    return new NumberPrimitiveFleakData(value, type);
  }

  private RecordFleakData deserializeObject(JsonParser p) throws IOException {
    Map<String, FleakData> payload = new HashMap<>();

    // Move past the START_OBJECT token
    JsonToken token = p.nextToken();

    // Process all fields until the end of the object
    while (token != JsonToken.END_OBJECT) {
      // We should be at a field name
      if (token != JsonToken.FIELD_NAME) {
        throw new IOException("Expected field name, found: " + token);
      }

      String fieldName = p.currentName();

      // Move to the field value
      token = p.nextToken();

      // Recursively deserialize the field value
      FleakData fieldValue = deserializeToken(p, token);
      payload.put(fieldName, fieldValue);

      // Move to the next token (either another field name or end of object)
      token = p.nextToken();
    }

    return new RecordFleakData(payload);
  }

  private ArrayFleakData deserializeArray(JsonParser p) throws IOException {
    List<FleakData> items = new ArrayList<>();

    // Move past the START_ARRAY token
    JsonToken token = p.nextToken();

    // Process all elements until the end of the array
    while (token != JsonToken.END_ARRAY) {
      // Recursively deserialize each array element
      FleakData item = deserializeToken(p, token);
      items.add(item);

      // Move to the next token (either another array element or end of array)
      token = p.nextToken();
    }

    return new ArrayFleakData(items);
  }
}
