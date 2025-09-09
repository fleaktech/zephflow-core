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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FleakDataSerializationTest {

  @Test
  public void testPrimitiveSerializationAndDeserialization() {
    // String serialization
    StringPrimitiveFleakData stringData = new StringPrimitiveFleakData("test value");
    String stringJson = JsonUtils.toJsonString(stringData);
    assertEquals("\"test value\"", stringJson);

    FleakData deserializedString = JsonUtils.loadFleakDataFromJsonString(stringJson);
    assertInstanceOf(StringPrimitiveFleakData.class, deserializedString);
    assertEquals("test value", deserializedString.getStringValue());

    // Boolean serialization
    BooleanPrimitiveFleakData booleanData = new BooleanPrimitiveFleakData(true);
    String booleanJson = JsonUtils.toJsonString(booleanData);
    assertEquals("true", booleanJson);

    FleakData deserializedBoolean = JsonUtils.loadFleakDataFromJsonString(booleanJson);
    assertInstanceOf(BooleanPrimitiveFleakData.class, deserializedBoolean);
    assertTrue(deserializedBoolean.isTrueValue());

    // Number serialization
    NumberPrimitiveFleakData intData =
        new NumberPrimitiveFleakData(42, NumberPrimitiveFleakData.NumberType.LONG);
    String intJson = JsonUtils.toJsonString(intData);
    assertEquals("42", intJson);

    FleakData deserializedInt = JsonUtils.loadFleakDataFromJsonString(intJson);
    assertInstanceOf(NumberPrimitiveFleakData.class, deserializedInt);
    assertEquals(42.0, deserializedInt.getNumberValue());

    NumberPrimitiveFleakData doubleData =
        new NumberPrimitiveFleakData(3.14, NumberPrimitiveFleakData.NumberType.DOUBLE);
    String doubleJson = JsonUtils.toJsonString(doubleData);
    assertEquals("3.14", doubleJson);

    FleakData deserializedDouble = JsonUtils.loadFleakDataFromJsonString(doubleJson);
    assertInstanceOf(NumberPrimitiveFleakData.class, deserializedDouble);
    assertEquals(3.14, deserializedDouble.getNumberValue());
  }

  @Test
  public void testArraySerializationAndDeserialization() {
    // Create an array of mixed types
    List<FleakData> arrayItems =
        Arrays.asList(
            new StringPrimitiveFleakData("item1"),
            new NumberPrimitiveFleakData(42, NumberPrimitiveFleakData.NumberType.LONG),
            new BooleanPrimitiveFleakData(true));
    ArrayFleakData arrayData = new ArrayFleakData(arrayItems);

    // Serialize
    String arrayJson = JsonUtils.toJsonString(arrayData);
    assertEquals("[\"item1\",42,true]", arrayJson);

    // Deserialize
    FleakData deserializedArray = JsonUtils.loadFleakDataFromJsonString(arrayJson);
    assertInstanceOf(ArrayFleakData.class, deserializedArray);

    List<FleakData> deserializedItems = deserializedArray.getArrayPayload();
    assertEquals(3, deserializedItems.size());

    assertInstanceOf(StringPrimitiveFleakData.class, deserializedItems.get(0));
    assertEquals("item1", deserializedItems.get(0).getStringValue());

    assertInstanceOf(NumberPrimitiveFleakData.class, deserializedItems.get(1));
    assertEquals(42.0, deserializedItems.get(1).getNumberValue());

    assertInstanceOf(BooleanPrimitiveFleakData.class, deserializedItems.get(2));
    assertTrue(deserializedItems.get(2).isTrueValue());
  }

  @Test
  public void testRecordSerializationAndDeserialization() {
    // Create a nested record structure
    Map<String, FleakData> payloadMap = new HashMap<>();
    payloadMap.put("name", new StringPrimitiveFleakData("John"));
    payloadMap.put(
        "age", new NumberPrimitiveFleakData(30, NumberPrimitiveFleakData.NumberType.LONG));
    payloadMap.put("active", new BooleanPrimitiveFleakData(true));

    // Add a nested array
    List<FleakData> hobbies =
        Arrays.asList(
            new StringPrimitiveFleakData("reading"), new StringPrimitiveFleakData("hiking"));
    payloadMap.put("hobbies", new ArrayFleakData(hobbies));

    // Add a nested record
    Map<String, FleakData> addressMap = new HashMap<>();
    addressMap.put("city", new StringPrimitiveFleakData("New York"));
    addressMap.put(
        "zip", new NumberPrimitiveFleakData(10001, NumberPrimitiveFleakData.NumberType.LONG));
    payloadMap.put("address", new RecordFleakData(addressMap));

    RecordFleakData recordData = new RecordFleakData(payloadMap);

    // Serialize
    String recordJson = JsonUtils.toJsonString(recordData);

    // The JSON should look like a regular object (without FleakData wrappers)
    String expectedJson =
        """
                {"name":"John","age":30,"active":true,"hobbies":["reading","hiking"],"address":{"city":"New York","zip":10001}}
                """
            .trim();

    // Normalize the JSON strings to handle potential formatting differences
    assertEquals(
        JsonUtils.fromJsonString(expectedJson, Object.class),
        JsonUtils.fromJsonString(recordJson, Object.class));

    // Deserialize
    FleakData deserializedRecord = JsonUtils.loadFleakDataFromJsonString(recordJson);
    assertInstanceOf(RecordFleakData.class, deserializedRecord);

    Map<String, FleakData> deserializedPayload = deserializedRecord.getPayload();
    assertEquals(5, deserializedPayload.size());

    // Check primitive fields
    assertInstanceOf(StringPrimitiveFleakData.class, deserializedPayload.get("name"));
    assertEquals("John", deserializedPayload.get("name").getStringValue());

    assertInstanceOf(NumberPrimitiveFleakData.class, deserializedPayload.get("age"));
    assertEquals(30.0, deserializedPayload.get("age").getNumberValue());

    assertInstanceOf(BooleanPrimitiveFleakData.class, deserializedPayload.get("active"));
    assertTrue(deserializedPayload.get("active").isTrueValue());

    // Check nested array
    assertInstanceOf(ArrayFleakData.class, deserializedPayload.get("hobbies"));
    List<FleakData> deserializedHobbies = deserializedPayload.get("hobbies").getArrayPayload();
    assertEquals(2, deserializedHobbies.size());
    assertEquals("reading", deserializedHobbies.get(0).getStringValue());
    assertEquals("hiking", deserializedHobbies.get(1).getStringValue());

    // Check nested record
    assertInstanceOf(RecordFleakData.class, deserializedPayload.get("address"));
    Map<String, FleakData> deserializedAddress = deserializedPayload.get("address").getPayload();
    assertEquals(2, deserializedAddress.size());
    assertEquals("New York", deserializedAddress.get("city").getStringValue());
    assertEquals(10001.0, deserializedAddress.get("zip").getNumberValue());
  }

  @Test
  public void testRoundTripConversion() {
    // Create a complex object with nested structures
    Map<String, Object> rawData = new HashMap<>();
    rawData.put("string", "test");
    rawData.put("number", 42);
    rawData.put("boolean", true);
    rawData.put("array", Arrays.asList("item1", 2, false));

    Map<String, Object> nestedObj = new HashMap<>();
    nestedObj.put("key1", "value1");
    nestedObj.put("key2", 123);
    rawData.put("object", nestedObj);

    // First, convert raw data to FleakData
    FleakData fleakData = FleakData.wrap(rawData);
    assertInstanceOf(RecordFleakData.class, fleakData);

    // Convert to JSON
    String json = JsonUtils.toJsonString(fleakData);

    // Convert back to FleakData
    FleakData roundTripData = JsonUtils.loadFleakDataFromJsonString(json);

    // Verify structure is preserved
    assertInstanceOf(RecordFleakData.class, roundTripData);
    RecordFleakData recordData = (RecordFleakData) roundTripData;

    Map<String, FleakData> payload = recordData.getPayload();
    assertEquals(5, payload.size());

    // Check primitive values
    assertEquals("test", payload.get("string").getStringValue());
    assertEquals(42.0, payload.get("number").getNumberValue());
    assertTrue(payload.get("boolean").isTrueValue());

    // Check array
    assertInstanceOf(ArrayFleakData.class, payload.get("array"));
    List<FleakData> arrayItems = payload.get("array").getArrayPayload();
    assertEquals(3, arrayItems.size());
    assertEquals("item1", arrayItems.get(0).getStringValue());
    assertEquals(2.0, arrayItems.get(1).getNumberValue());
    assertFalse(arrayItems.get(2).isTrueValue());

    // Check nested object
    assertInstanceOf(RecordFleakData.class, payload.get("object"));
    Map<String, FleakData> nestedPayload = payload.get("object").getPayload();
    assertEquals(2, nestedPayload.size());
    assertEquals("value1", nestedPayload.get("key1").getStringValue());
    assertEquals(123.0, nestedPayload.get("key2").getNumberValue());

    // Finally convert back to native Java objects and compare
    Object convertedBack = roundTripData.unwrap();
    assertEquals(rawData.size(), ((Map<?, ?>) convertedBack).size());
  }
}
