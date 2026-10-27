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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ElasticsearchRawDataConverterTest {

  record Increase(String counter, long n, Map<String, String> tags) {}

  private final List<Increase> increases = new ArrayList<>();

  static final List<String> SOURCES =
      List.of(
          json("{'small':1,'negative':-7,'long':9007199254740993,'maxLong':9223372036854775807}"),
          json("{'beyondLong':123456789012345678901234567890,'digits400':" + "9".repeat(400) + "}"),
          json("{'decimal':1.5,'one':1.0,'exponent':-1.5e3,'big':1e300,'subnormal':4.9e-324}"),
          json("{'underflow':1e-400,'negativeZero':-0.0,'upper':1E+2,'small':0.1e-2}"),
          json("{'nul':null,'yes':true,'no':false,'text':'中文 é 😀','escaped':'\\u00e9\\n'}"),
          json("{'nested':{'a':[1,{'b':[]},{}],'c':{'d':null}},'empty':[],'emptyObject':{}}"),
          json("{'hits':{'hits':[{'_source':1}]},'_scroll_id':'X','é':{ 'w' : [ 1 , 2 ] }}"),
          json("{'dup':1,'dup':'second'}"),
          json("{'_id':'clash','_index':7,'a':1}"),
          "{}");

  static final List<List<Map<String, Object>>> RECORDS =
      List.of(
          List.of(
              Map.of(
                  "small",
                  1L,
                  "negative",
                  -7L,
                  "long",
                  9007199254740992L,
                  "maxLong",
                  9223372036854775807L,
                  "_id",
                  "id-1",
                  "_index",
                  "idx")),
          List.of(
              Map.of(
                  "beyondLong",
                  1.2345678901234568E29,
                  "digits400",
                  Double.POSITIVE_INFINITY,
                  "_id",
                  "id-1",
                  "_index",
                  "idx")),
          List.of(
              Map.of(
                  "decimal",
                  1.5,
                  "one",
                  1.0,
                  "exponent",
                  -1500.0,
                  "big",
                  1.0E300,
                  "subnormal",
                  4.9E-324,
                  "_id",
                  "id-1",
                  "_index",
                  "idx")),
          List.of(
              Map.of(
                  "underflow",
                  0.0,
                  "negativeZero",
                  -0.0,
                  "upper",
                  100.0,
                  "small",
                  0.001,
                  "_id",
                  "id-1",
                  "_index",
                  "idx")),
          List.of(
              nullable(
                  "nul", null, "yes", true, "no", false, "text", "中文 é 😀", "escaped", "é\n", "_id",
                  "id-1", "_index", "idx")),
          List.of(
              Map.of(
                  "nested",
                  Map.of(
                      "a", List.of(1L, Map.of("b", List.of()), Map.of()), "c", nullable("d", null)),
                  "empty",
                  List.of(),
                  "emptyObject",
                  Map.of(),
                  "_id",
                  "id-1",
                  "_index",
                  "idx")),
          List.of(
              Map.of(
                  "hits",
                  Map.of("hits", List.of(Map.of("_source", 1L))),
                  "_scroll_id",
                  "X",
                  "é",
                  Map.of("w", List.of(1L, 2L)),
                  "_id",
                  "id-1",
                  "_index",
                  "idx")),
          List.of(Map.of("dup", "second", "_id", "id-1", "_index", "idx")),
          List.of(Map.of("a", 1L, "_id", "id-1", "_index", "idx")),
          List.of(Map.of("_id", "id-1", "_index", "idx")));

  static Stream<Arguments> sources() {
    return IntStream.range(0, SOURCES.size())
        .mapToObj(i -> Arguments.of(SOURCES.get(i), RECORDS.get(i)));
  }

  @ParameterizedTest
  @MethodSource("sources")
  void recordsKeepTheirJsonTypes(String source, List<Map<String, Object>> expected)
      throws Exception {
    assertEquals(expected, converted(document(source)));
  }

  @Test
  void aNumberBeyondTheDoubleRangeStaysANumber() throws Exception {
    ElasticsearchDocument doc = document(json("{'d':1e400,'n':-1e400}"));

    assertEquals(
        List.of(
            Map.of(
                "d",
                Double.POSITIVE_INFINITY,
                "n",
                Double.NEGATIVE_INFINITY,
                "_id",
                "id-1",
                "_index",
                "idx")),
        converted(doc));
  }

  @Test
  void countsTheSourceSizeAndOneEventWithTheRecordTags() {
    Map<String, Object> source = Map.of("a", 1, "__tag__", Map.of("team", "x"));
    ElasticsearchDocument doc = new ElasticsearchDocument("1", "idx", source, 42);

    ConvertedResult<ElasticsearchDocument> result =
        new ElasticsearchRawDataConverter().convert(doc, context());

    assertEquals(
        List.of(
            ConvertedResult.success(
                List.of(
                    (RecordFleakData)
                        FleakData.wrap(
                            Map.of(
                                "a",
                                1,
                                "__tag__",
                                Map.of("team", "x"),
                                "_id",
                                "1",
                                "_index",
                                "idx"))),
                doc),
            List.of(
                new Increase("size", 42, Map.of("team", "x")),
                new Increase("count", 1, Map.of("team", "x")))),
        List.of(result, increases));
  }

  @Test
  void aConversionThatThrowsFailsTheHit() {
    ElasticsearchDocument doc = new ElasticsearchDocument("1", "idx", null, 4);

    ConvertedResult<ElasticsearchDocument> result =
        new ElasticsearchRawDataConverter().convert(doc, context());

    assertEquals(
        Arrays.asList(
            null, doc, NullPointerException.class, List.of(new Increase("failure", 1, Map.of()))),
        Arrays.asList(
            result.transformedData(), result.sourceRecord(), result.error().getClass(), increases));
  }

  @SuppressWarnings("unchecked")
  private static ElasticsearchDocument document(String source) throws Exception {
    return new ElasticsearchDocument(
        "id-1", "idx", OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree(source), Map.class));
  }

  private static Map<String, Object> nullable(Object... keysAndValues) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  private List<Map<String, Object>> converted(ElasticsearchDocument doc) {
    return new ElasticsearchRawDataConverter()
        .convert(doc, context()).transformedData().stream().map(RecordFleakData::unwrap).toList();
  }

  private SourceExecutionContext<ElasticsearchDocument> context() {
    return new SourceExecutionContext<>(
        null, null, null, counter("size"), counter("count"), counter("failure"), null);
  }

  private FleakCounter counter(String name) {
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
}
