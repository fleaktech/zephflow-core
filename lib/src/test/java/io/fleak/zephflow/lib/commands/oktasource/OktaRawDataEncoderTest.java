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
package io.fleak.zephflow.lib.commands.oktasource;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OktaRawDataEncoderTest {

  @Test
  void serializesPayloadToJsonBytes() {
    var encoder = new OktaRawDataEncoder();
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("uuid", "evt-1");
    payload.put("eventType", "user.session.start");
    OktaLogEvent event = new OktaLogEvent(payload, "evt-1");

    var serialized = encoder.serialize(event);

    assertNotNull(serialized);
    assertNull(serialized.key());
    assertNull(serialized.metadata());
    String json = new String(serialized.value());
    assertTrue(json.contains("\"uuid\":\"evt-1\""));
    assertTrue(json.contains("\"eventType\":\"user.session.start\""));
  }

  @Test
  void throwsRuntimeExceptionWhenPayloadIsNotSerializable() {
    var encoder = new OktaRawDataEncoder();
    Map<String, Object> payload = new LinkedHashMap<>();
    // Self-referential map cannot be serialized to JSON
    payload.put("self", payload);
    OktaLogEvent event = new OktaLogEvent(payload, "evt-1");

    RuntimeException ex = assertThrows(RuntimeException.class, () -> encoder.serialize(event));
    assertTrue(ex.getMessage().contains("Failed to serialize Okta event"));
  }
}
