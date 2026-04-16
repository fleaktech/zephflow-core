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
package io.fleak.zephflow.lib.commands.ldapsource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LdapRawDataEncoderTest {

  @Test
  void testSerialize() {
    var encoder = new LdapRawDataEncoder();

    Map<String, List<String>> attributes = new LinkedHashMap<>();
    attributes.put("cn", List.of("John Doe"));
    attributes.put("mail", List.of("jdoe@example.com"));

    LdapEntry entry = new LdapEntry("uid=jdoe,ou=users,dc=example,dc=com", attributes);

    SerializedEvent serializedEvent = encoder.serialize(entry);

    assertNotNull(serializedEvent);
    assertNotNull(serializedEvent.key());
    assertNotNull(serializedEvent.value());
    assertEquals(
        "uid=jdoe,ou=users,dc=example,dc=com",
        new String(serializedEvent.key(), StandardCharsets.UTF_8));
    assertTrue(serializedEvent.value().length > 0);

    String json = new String(serializedEvent.value(), StandardCharsets.UTF_8);
    assertTrue(json.contains("uid=jdoe,ou=users,dc=example,dc=com"));
    assertTrue(json.contains("John Doe"));
    assertTrue(json.contains("jdoe@example.com"));
  }
}
