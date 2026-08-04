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

import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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

  private static final String DOC_A = "{\"a\":1}";
  private static final String DOC_B = "{\"b\":2}";

  /** Serves one canned _bulk response and records the request body it received. */
  private record FakeEs(HttpServer server, AtomicReference<byte[]> lastBody)
      implements AutoCloseable {
    static FakeEs serving(String bulkResponseJson) throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      AtomicReference<byte[]> lastBody = new AtomicReference<>();
      server.createContext(
          "/_bulk",
          exchange -> {
            lastBody.set(exchange.getRequestBody().readAllBytes());
            byte[] out = bulkResponseJson.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, out.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(out);
            }
          });
      server.start();
      return new FakeEs(server, lastBody);
    }

    String host() {
      return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }

  private static SimpleSinkCommand.PreparedInputEvents<ElasticsearchOutboundDoc> twoDocs() {
    RecordFleakData raw = (RecordFleakData) FleakData.wrap(Map.of("k", "v"));
    SimpleSinkCommand.PreparedInputEvents<ElasticsearchOutboundDoc> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(raw, new ElasticsearchOutboundDoc(DOC_A));
    events.add(raw, new ElasticsearchOutboundDoc(DOC_B));
    return events;
  }

  @Test
  void flushAllIndexed_reportsFullBodySize() throws Exception {
    try (FakeEs es = FakeEs.serving("{\"errors\":false,\"items\":[]}")) {
      ElasticsearchSinkFlusher flusher =
          new ElasticsearchSinkFlusher(es.host(), "idx", null, null, Map.of());

      SimpleSinkCommand.FlushResult result = flusher.flush(twoDocs(), Map.of());

      assertEquals(2, result.successCount());
      assertEquals(es.lastBody().get().length, result.flushedDataSize());
    }
  }

  /**
   * On a partial failure the flusher used to report the entire request body, crediting bytes for
   * documents Elasticsearch rejected. It must report only the NDJSON of the indexed ones.
   */
  @Test
  void flushPartialFailure_reportsOnlyIndexedDocBytes() throws Exception {
    String bulkResponse =
        "{\"errors\":true,\"items\":["
            + "{\"index\":{\"status\":201}},"
            + "{\"index\":{\"status\":400,\"error\":{\"reason\":\"mapper_parsing_exception\"}}}"
            + "]}";
    try (FakeEs es = FakeEs.serving(bulkResponse)) {
      ElasticsearchSinkFlusher flusher =
          new ElasticsearchSinkFlusher(es.host(), "idx", null, null, Map.of());

      SimpleSinkCommand.FlushResult result = flusher.flush(twoDocs(), Map.of());

      assertEquals(1, result.successCount());
      assertEquals(1, result.errorOutputList().size());

      int fullBody = es.lastBody().get().length;
      // action-meta line + newline + the one accepted doc + newline.
      int actionMeta = "{\"index\":{\"_index\":\"idx\"}}".getBytes(StandardCharsets.UTF_8).length;
      long expected = actionMeta + 1 + DOC_A.getBytes(StandardCharsets.UTF_8).length + 1;
      assertEquals(expected, result.flushedDataSize());
      assertTrue(
          result.flushedDataSize() < fullBody, "must not credit bytes for the rejected document");
    }
  }
}
