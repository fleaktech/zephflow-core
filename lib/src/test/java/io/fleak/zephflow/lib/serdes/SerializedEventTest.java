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
package io.fleak.zephflow.lib.serdes;

import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_KEY;
import static io.fleak.zephflow.lib.utils.MiscUtils.toBase64String;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/17/24 */
class SerializedEventTest {

  @Test
  void testMetadataWithKey_whenKeyIsPresent() {
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    Map<String, String> metadata = new HashMap<>();
    metadata.put("eventType", "testEvent");

    SerializedEvent event = new SerializedEvent(key, value, metadata);

    Map<String, String> result = SerializedEvent.metadataWithKey(event);

    assertEquals(Map.of("eventType", "testEvent", METADATA_KEY, toBase64String(key)), result);
  }

  @Test
  void testMetadataWithKey_whenKeyIsNull() {
    byte[] value = "testValue".getBytes();
    Map<String, String> metadata = new HashMap<>();
    metadata.put("eventType", "testEvent");

    SerializedEvent event = new SerializedEvent(null, value, metadata);

    Map<String, String> result = SerializedEvent.metadataWithKey(event);

    assertEquals(Map.of("eventType", "testEvent"), result);
  }

  @Test
  void testMetadataWithKey_whenMetadataIsNull() {
    byte[] value = "testValue".getBytes();
    SerializedEvent event = new SerializedEvent(null, value, null);
    Map<String, String> result = SerializedEvent.metadataWithKey(event);
    assertTrue(result.isEmpty());
  }

  @Test
  void testCreate_whenMetadataWithKeyContainsKey() {
    byte[] value = "testValue".getBytes();
    Map<String, String> metadataWithKey = new HashMap<>();
    metadataWithKey.put(METADATA_KEY, toBase64String("testKey".getBytes()));
    metadataWithKey.put("eventType", "testEvent");

    // Call the method under test
    SerializedEvent event = SerializedEvent.create(metadataWithKey, value);

    // Verify that the key is correctly extracted from base64
    assertArrayEquals("testKey".getBytes(), event.key());
    assertArrayEquals(value, event.value());
    assertEquals(1, event.metadata().size());
    assertEquals("testEvent", event.metadata().get("eventType"));
  }

  @Test
  void testCreate_whenMetadataWithKeyIsNull() {
    byte[] value = "testValue".getBytes();

    // Call the method under test
    SerializedEvent event = SerializedEvent.create(null, value);

    // Verify that the key and metadata are null
    assertNull(event.key());
    assertArrayEquals(value, event.value());
    assertNull(event.metadata());
  }

  @Test
  void testCreate_whenMetadataWithKeyDoesNotContainKey() {
    byte[] value = "testValue".getBytes();
    Map<String, String> metadataWithKey = new HashMap<>();
    metadataWithKey.put("eventType", "testEvent");

    // Call the method under test
    SerializedEvent event = SerializedEvent.create(metadataWithKey, value);

    // Verify that the key is null and metadata is correctly populated
    assertNull(event.key());
    assertArrayEquals(value, event.value());
    assertEquals(1, event.metadata().size());
    assertEquals("testEvent", event.metadata().get("eventType"));
  }
}
