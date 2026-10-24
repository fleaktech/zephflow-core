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
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_DESER_ERR_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_SIZE_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.elasticsearchsource.ElasticsearchStub.Request;
import io.fleak.zephflow.lib.commands.elasticsearchsource.ElasticsearchStub.Response;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class ElasticsearchSourceCommandTest {

  record Increase(String counter, long n, Map<String, String> tags) {}

  private final List<Increase> increases = new ArrayList<>();

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"JSON_OBJECT", "TEXT"})
  void scrollsTheIndexIntoRecords(String encodingType) throws Exception {
    String auth =
        "Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes(StandardCharsets.UTF_8));
    List<Object> records = new ArrayList<>();
    List<Request> requests;
    try (var stub =
        new ElasticsearchStub(
            new Response(
                200,
                json(
                    "{'_scroll_id':'S1','hits':{'hits':["
                        + "{'_id':'1','_index':'idx','_source':{'a':1,'__tag__':{'team':'x'}}},"
                        + "{'_id':'2','_index':'idx','_source':{ 'b' : 'é' }}]}}")),
            new Response(
                200,
                json(
                    "{'_scroll_id':'S2','hits':{'hits':["
                        + "{'_id':'3','_index':'idx','_source':{'c':[1.5]}}]}}")),
            new Response(200, json("{'_scroll_id':'S3','hits':{'hits':[]}}")))) {
      var command =
          new ElasticsearchSourceCommandFactory()
              .createCommand(
                  "node",
                  TestUtils.buildJobContext(
                      new HashMap<>(
                          Map.of("es-cred", new UsernamePasswordCredential("user", "pass")))));
      Map<String, Object> config =
          new HashMap<>(
              Map.of(
                  "host",
                  stub.url() + "/",
                  "index",
                  "idx",
                  "query",
                  json("{'term':{'a':1}}"),
                  "scrollTimeout",
                  "1m",
                  "batchSize",
                  2,
                  "credentialId",
                  "es-cred"));
      if (encodingType != null) {
        config.put("encodingType", encodingType);
      }
      command.parseAndValidateArg(config);
      command.initialize(new RecordingMetricClientProvider());
      command.execute(
          "user",
          new SourceEventAcceptor() {
            @Override
            public void accept(List<RecordFleakData> events) {
              events.forEach(e -> records.add(e.unwrap()));
            }

            @Override
            public void terminate() {}
          });
      requests = stub.requests();
    }

    assertEquals(
        List.of(
            List.of(
                Map.of("a", 1L, "__tag__", Map.of("team", "x"), "_id", "1", "_index", "idx"),
                Map.of("b", "é", "_id", "2", "_index", "idx"),
                Map.of("c", List.of(1.5), "_id", "3", "_index", "idx")),
            List.of(
                new Increase(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, 30, Map.of("team", "x")),
                new Increase(METRIC_NAME_INPUT_EVENT_COUNT, 1, Map.of("team", "x")),
                new Increase(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, 14, Map.of()),
                new Increase(METRIC_NAME_INPUT_EVENT_COUNT, 1, Map.of()),
                new Increase(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, 11, Map.of()),
                new Increase(METRIC_NAME_INPUT_EVENT_COUNT, 1, Map.of())),
            List.of(
                new Request(
                    "POST",
                    "/idx/_search",
                    "scroll=1m",
                    json("{'size':2,'query':{'term':{'a':1}}}"),
                    auth),
                new Request(
                    "POST",
                    "/_search/scroll",
                    null,
                    json("{'scroll':'1m','scroll_id':'S1'}"),
                    auth),
                new Request(
                    "POST",
                    "/_search/scroll",
                    null,
                    json("{'scroll':'1m','scroll_id':'S2'}"),
                    auth))),
        List.of(
            records,
            increases.stream()
                .filter(
                    i ->
                        Set.of(
                                METRIC_NAME_INPUT_EVENT_SIZE_COUNT,
                                METRIC_NAME_INPUT_EVENT_COUNT,
                                METRIC_NAME_INPUT_DESER_ERR_COUNT)
                            .contains(i.counter()))
                .toList(),
            requests.stream().filter(r -> r.method().equals("POST")).toList()));
    assertTrue(
        requests.contains(
            new Request("DELETE", "/_search/scroll", null, json("{'scroll_id':['S3']}"), auth)),
        requests::toString);
  }

  private class RecordingMetricClientProvider implements MetricClientProvider {
    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          increase(1, additionalTags);
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          increases.add(new Increase(name, n, additionalTags));
        }
      };
    }

    @Override
    public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
      return new MetricClientProvider.NoopMetricClientProvider().gauge(name, tags, monitoredValue);
    }

    @Override
    public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
      return new MetricClientProvider.NoopMetricClientProvider().stopWatch(name, tags);
    }

    @Override
    public void close() {}
  }
}
