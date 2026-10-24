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
package io.fleak.zephflow.lib.commands.elasticsearchsource;

import static io.fleak.zephflow.lib.commands.elasticsearchsource.ElasticsearchStub.json;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.lib.commands.elasticsearchsource.ElasticsearchStub.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ElasticsearchSourceFetcherTest {

  /** A response body and the UTF-8 length of each hit's {@code _source} text in it. */
  record Body(String body, List<Long> sourceSizes) {}

  static Stream<Body> bodies() {
    return Stream.of(
        new Body(
            json(
                "{'_scroll_id':'S','hits':{'total':3,'hits':[{'_id':'1','_index':'i','_source':"
                    + "{'a':1,'é':'中文','n':-1.5e3}},{'_source' : { 'b' : [1, 2] } ,'_id':null},"
                    + "{'_id':5,'_source':'str'},{'_source':null},{},7,"
                    + "{'_source':{'hits':{'hits':[]},'_scroll_id':'X'}}]}}"),
            List.of(32L, 16L, 5L, 4L, 0L, 0L, 37L)),
        new Body(json("{'hits':{'hits':[]},'_scroll_id':'S2'}"), List.of()),
        new Body(
            json(
                "{'hits':{'hits':[{'_id':'first','_source':{'a':1}}]},"
                    + "'hits':{'hits':[{'_source':{'bb':22}}]}}"),
            List.of(9L)),
        new Body(json("{'hits':[1,2]}"), List.of()),
        new Body(json("{'hits':{'hits':{'x':1}}}"), List.of()),
        new Body(json("{'_source':{'a':1}}"), List.of()),
        new Body(
            json(
                "{'hits':{'hits':[{'_source':"
                    + "{'big':123456789012345678901234567890,'d':1e400,'i':1,'dup':1,'dup':2}}]}}"),
            List.of(70L)),
        new Body(
            json(
                "{'hits':{'hits':[{'_id':'e','_source':{'s':'\\u00e9\\n','t':'é😀','n':1e2}},"
                    + "{'_source':{ 'a' : [ 1 , { } ] }}]}}"),
            List.of(37L, 21L)),
        new Body(
            json(
                "{'hits':{'hits':[{'_source':0},{'_source':-0},{'_source':true},{'_source':1},"
                    + "{'_source':1.0}]}}"),
            List.of(1L, 2L, 4L, 1L, 3L)),
        new Body(
            json("{'hits':{'hits':[{'_source':{'first':1},'_id':'d','_source':{'b':2}}]}}"),
            List.of(7L)),
        new Body(
            json(
                "{'took':1,'hits':{'max_score':null,'hits':[{'_index':'i','_id':'x','_score':1.0,"
                    + "'_source':{'k':'v'},'fields':{'hits':[1]}}]},'_shards':{'total':1}}"),
            List.of(9L)),
        new Body("[1]", List.of()));
  }

  @ParameterizedTest
  @MethodSource("bodies")
  void treeIsTheOneReadTreeBuilds(Body body) throws Exception {
    assertEquals(
        OBJECT_MAPPER.readTree(body.body()),
        ElasticsearchSourceFetcher.readResponse(body.body()).root());
  }

  @ParameterizedTest
  @MethodSource("bodies")
  void documentsAreTodaysWithTheirSourceSizes(Body body) throws Exception {
    try (var stub = new ElasticsearchStub(new Response(200, body.body()));
        var fetcher = fetcher(stub)) {
      assertEquals(referenceDocuments(body), fetcher.fetch());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"{\"hits\":{\"hits\":[]}} xyz", "{\"hits\":{\"hits\":[]}}{}"})
  void trailingContentFailsAsReadTreeDoes(String body) {
    assertThrows(JsonProcessingException.class, () -> OBJECT_MAPPER.readTree(body));
    assertThrows(
        JsonProcessingException.class, () -> ElasticsearchSourceFetcher.readResponse(body));
  }

  @Test
  void aFailedSearchFailsTheFetch() throws Exception {
    try (var stub = new ElasticsearchStub(new Response(500, "boom"));
        var fetcher = fetcher(stub)) {
      RuntimeException e = assertThrows(RuntimeException.class, fetcher::fetch);
      assertEquals(
          List.of(
              "Failed to fetch from Elasticsearch",
              "Elasticsearch initial search failed with status 500: boom"),
          List.of(e.getMessage(), e.getCause().getMessage()));
    }
  }

  private static ElasticsearchSourceFetcher fetcher(ElasticsearchStub stub) {
    return new ElasticsearchSourceFetcher(stub.url(), "idx", null, "1m", 10, null, null);
  }

  /** The documents the fetcher built before it measured sizes, from {@code readTree}. */
  @SuppressWarnings("unchecked")
  private static List<ElasticsearchDocument> referenceDocuments(Body body) throws IOException {
    JsonNode hitsNode = OBJECT_MAPPER.readTree(body.body()).path("hits").path("hits");
    if (!hitsNode.isArray() || hitsNode.isEmpty()) {
      return List.of();
    }
    List<ElasticsearchDocument> docs = new ArrayList<>();
    for (JsonNode hit : hitsNode) {
      JsonNode sourceNode = hit.path("_source");
      Map<String, Object> source =
          sourceNode.isObject()
              ? OBJECT_MAPPER.convertValue(sourceNode, Map.class)
              : new LinkedHashMap<>();
      docs.add(
          new ElasticsearchDocument(
              hit.path("_id").asText(),
              hit.path("_index").asText(),
              source,
              body.sourceSizes().get(docs.size())));
    }
    return docs;
  }
}
