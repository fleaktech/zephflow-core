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
package io.fleak.zephflow.lib.commands.elasticsearchsink;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ElasticsearchSinkFlusherTest {

  @Test
  void buildQueryString_nullParams_returnsEmpty() {
    assertEquals("", ElasticsearchSinkFlusher.buildQueryString(null));
  }

  @Test
  void buildQueryString_emptyMap_returnsEmpty() {
    assertEquals("", ElasticsearchSinkFlusher.buildQueryString(Map.of()));
  }

  @Test
  void buildQueryString_singleEntryWithCommaInValue_encodesCorrectly() {
    Map<String, String> params = Map.of("_stream_fields", "service,env,namespace");
    assertEquals(
        "?_stream_fields=service%2Cenv%2Cnamespace",
        ElasticsearchSinkFlusher.buildQueryString(params));
  }

  @Test
  void buildQueryString_twoEntries_containsBothPairs() {
    Map<String, String> params = new LinkedHashMap<>();
    params.put("foo", "bar");
    params.put("baz", "qux");
    String result = ElasticsearchSinkFlusher.buildQueryString(params);
    assertTrue(result.startsWith("?"), "Should start with '?'");
    assertTrue(result.contains("foo=bar"), "Should contain foo=bar");
    assertTrue(result.contains("baz=qux"), "Should contain baz=qux");
    assertTrue(result.contains("&"), "Should contain '&' separator");
  }
}
