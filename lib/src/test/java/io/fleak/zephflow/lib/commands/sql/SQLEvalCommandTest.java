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
package io.fleak.zephflow.lib.commands.sql;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonResource;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.sql.errors.SQLSyntaxError;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SQLEvalCommandTest {

  private MetricClientProvider metricClientProvider;

  private FleakCounter inputMessageCounter;
  private FleakCounter outputMessageCounter;
  private FleakCounter errorCounter;

  private final Map<String, String> tags =
      Map.of(
          METRIC_TAG_COMMAND_NAME, COMMAND_NAME_SQL_EVAL,
          METRIC_TAG_NODE_ID, "myNodeId",
          METRIC_TAG_SERVICE, "my_service",
          METRIC_TAG_ENV, "my_env");

  @BeforeEach
  public void beforeEach() {
    metricClientProvider = mock(MetricClientProvider.class);
    inputMessageCounter = mock(FleakCounter.class);
    outputMessageCounter = mock(FleakCounter.class);
    errorCounter = mock(FleakCounter.class);
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_COUNT), eq(tags)))
        .thenReturn(inputMessageCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_COUNT), eq(tags)))
        .thenReturn(outputMessageCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), eq(tags)))
        .thenReturn(errorCounter);
  }

  @Test
  public void testAlphaUseCaseOne() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/eval_alpha_use_case_one.json");
    String arg = "select content from records";
    testSqlProcess(
        inputEvent,
        arg,
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "content",
                    "Apple is moving to in-house 5G modem chips for its 2024 iPhones, as far as the chief executive of Qualcomm — which currently produces them for the tech giant — is aware.\n\n\"We're making no plans for 2024, my planning assumption is we're not providing [Apple] a modem in '24, but it's their decision to make,\" Cristiano Amon told CNBC at the Mobile World Congress in Barcelona.\n\nApple's most recent iPhone 14 models use Qualcomm modems, but the company has been looking to go solo in the wireless connectivity market for some years.\n\nIt bought Intel's modem business in 2019 and there had been speculation it would begin using in-house parts this year.\n\nAmon said Qualcomm had told investors back in 2021 that it did not expect to provide modems for the iPhone in 2023, but Apple then decided to continue for another year.\n\nQualcomm has been diversifying its business into automotive semiconductors and low-power applications.")));
  }

  @Test
  public void testAlphaUseCaseTwo() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/eval_alpha_use_case_two.json");
    String arg =
        """
                        SELECT
                            article ->> 'text' AS "text"
                        FROM
                            records,
                            json_array_elements(articles) AS article;
                        """;
    testSqlProcess(
        inputEvent,
        arg,
        List.of(
            (RecordFleakData)
                FleakData.wrap(
                    Map.of(
                        "text",
                        "MEMs packaging plays a vital role in the protection of the wafer and chipset structure from environmental factors along with providing other benefits such as conductivity, connective communication, etc. The complex manufacturing process involved in MEMs packaging is restraining the")),
            (RecordFleakData)
                FleakData.wrap(
                    Map.of(
                        "text",
                        "Don't Quarantine Your Research, you keep your social distance and we provide you a social DISCOUNT use QUARANTINEDAYS Code in precise requirement and Get FLAT 1000USD OFF on all CMI reports Market Overview Haptic technology is also known as kinaesthetic communication or 3D touch,"))));
  }

  @Test
  public void testAlphaUseCaseThree() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/eval_alpha_use_case_three.json");
    String arg =
        """
              SELECT
                  question,
                  string_agg(match->'metadata'->>'text', '$$') AS context
              FROM
                  records,
                  json_array_elements(news_query_result->'matchList') AS match
              GROUP BY
                  question;
              """;
    testSqlProcess(
        inputEvent,
        arg,
        List.of(
            (RecordFleakData)
                FleakData.wrap(
                    Map.of(
                        "question",
                        "test question?",
                        "context",
                        "news 1 about Argentina won the world cup 2022$$news 2 about Argentina won the world cup 2022"))));
  }

  @Test
  public void testRegexPSplit() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/regexp.json");
    String arg =
        """
                  SELECT
                      regexp_split_to_array(data, ',') output
                  FROM
                      records
                  """;
    testSqlProcess(
        inputEvent,
        arg,
        List.of((RecordFleakData) FleakData.wrap(Map.of("output", List.of("1", "2", "3", "4")))));
  }

  @Test
  public void testRegexPSplitTable() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/regexp.json");
    String arg =
        """
                  SELECT
                      count(*) c
                  FROM
                      records,
                      regexp_split_to_table(data, ',')
                  """;
    testSqlProcess(inputEvent, arg, List.of((RecordFleakData) FleakData.wrap(Map.of("c", 4L))));
  }

  @Test
  public void testSelectColumn() throws Exception {
    // this input message have `summary` field being null
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/str_replace_error2.json");
    String arg = "select country, 'test' as media from records";
    testSqlProcess(
        inputEvent,
        arg,
        (RecordFleakData) FleakData.wrap(Map.of("country", "US", "media", "test")));
  }

  @Test
  void testSelectColumn2() {
    RecordFleakData inputEvent =
        (RecordFleakData) FleakData.wrap(Map.of("value", 100, "foo", "abc"));
    String arg = "select value*2 as val2 from records";
    testSqlProcess(inputEvent, arg, (RecordFleakData) FleakData.wrap(Map.of("val2", 200L)));
  }

  @Test
  void testSelectColumWithoutRename() {
    RecordFleakData inputEvent =
        (RecordFleakData) FleakData.wrap(Map.of("value", 100, "foo", "abc"));
    String argWithoutRename = "select value*2 from records";
    testSqlProcess(
        inputEvent, argWithoutRename, (RecordFleakData) FleakData.wrap(Map.of("col_1", 200L)));
  }

  @Test
  void testJson() throws IOException {
    RecordFleakData recordFleakData =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_with_stringified_json.json");
    String arg =
        """
SELECT
    json_data ->> 'ticker' AS ticker,
    json_data ->> 'label' AS label
FROM
    records,
    json_array_elements((semtiment_labels->'modelOutputs'->0->>'result')::json) AS json_data;""";
    testSqlProcess(
        recordFleakData,
        arg,
        List.of(
            (RecordFleakData) FleakData.wrap(Map.of("ticker", "AAPL", "label", "positive")),
            (RecordFleakData) FleakData.wrap(Map.of("ticker", "QCOM", "label", "neutral")),
            (RecordFleakData) FleakData.wrap(Map.of("ticker", "INTC", "label", "neutral"))));
  }

  @Test
  public void testInvalidSql_inputNotFullyParsed() {
    String arg = "select a, unknown_func(b) from my_table";
    SQLEvalCommand sqlEvalCommand = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);
    Exception e =
        assertThrows(
            SQLSyntaxError.class, () -> sqlEvalCommand.parseAndValidateArg(Map.of("sql", arg)));
    assertTrue(e.getMessage().startsWith("Encountered parsing error"));
  }

  @Test
  public void testJsonBuildObject() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_embedding_result.json");
    String arg =
        """
SELECT
    embedding_output -> 'embedding' AS embedding,
    json_build_object('text', txt) AS metadata
FROM
    records""";
    testSqlProcess(
        inputEvent,
        arg,
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "embedding",
                    List.of(0.035992317d, 0.015277487d),
                    "metadata",
                    Map.of("text", "some text"))));
  }

  @Test
  public void testJsonGetFailuresShouldBeNullResult() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_json_get.json");

    var sqlQueries =
        new String[] {
          "select modelOutputs->'result-bla' v from records",
          "select modelOutputs->>'result-bla' v from records",
          "select modelOutputs->>'result-bla' v from records",
        };

    for (var sql : sqlQueries) {
      SQLEvalCommand sqlEvalCommand =
          new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);
      sqlEvalCommand.parseAndValidateArg(Map.of("sql", sql));
      sqlEvalCommand.initialize(metricClientProvider);
      var context = sqlEvalCommand.getExecutionContext();
      var result = sqlEvalCommand.process(List.of(inputEvent), null, context);
      assertTrue(result.getFailureEvents().isEmpty());
      assertEquals(1, result.getOutput().size());
      assertNull(result.getOutput().get(0).getPayload().get("v"));
    }
  }

  @Test
  public void testJsonGet() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_json_get.json");
    String arg =
        """
SELECT
    json_data ->> 'ticker' AS ticker,
    json_data ->> 'label' AS label
FROM
    records,
    json_array_elements((modelOutputs->0->>'result')::json) AS json_data;
""";
    SQLEvalCommand sqlEvalCommand = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);

    sqlEvalCommand.parseAndValidateArg(Map.of("sql", arg));
    sqlEvalCommand.initialize(metricClientProvider);
    var context = sqlEvalCommand.getExecutionContext();
    var output = sqlEvalCommand.process(List.of(inputEvent), null, context);
    assertTrue(output.getFailureEvents().isEmpty());
  }

  @Test
  public void testJsonGet_shouldErrorOutOnInvalidData() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_json_get_2.json");
    String arg =
        """
SELECT
    json_data ->> 'ticker' AS ticker,
    json_data ->> 'label' AS label
FROM
    records,
    json_array_elements((semtiment_labels->'modelOutputs'->0->>'result')::json) AS json_data;""";
    SQLEvalCommand sqlEvalCommand = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);

    sqlEvalCommand.parseAndValidateArg(Map.of("sql", arg));
    sqlEvalCommand.initialize(metricClientProvider);
    var context = sqlEvalCommand.getExecutionContext();
    var output = sqlEvalCommand.process(List.of(inputEvent), null, context);
    assertTrue(output.getOutput().isEmpty());
    assertEquals(1, output.getFailureEvents().size());
    assertEquals(
        """
cannot cast {"ticker": "AAPL", "label": "positive"}, {"ticker": "QCOM", "label": "negative"}, {"ticker": "INTC", "label": "neutral"} to a JsonNode""",
        output.getFailureEvents().get(0).errorMessage());
  }

  @Test
  public void testStringLike() throws IOException {
    RecordFleakData inputEvent =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/simple_event.json");
    String arg = "select * from records where name like 'Rick%';";
    testSqlProcess(inputEvent, arg, inputEvent);
  }

  @Test
  public void testStringLikeNotMatch() throws IOException {
    var inputEvent = (RecordFleakData) loadFleakDataFromJsonResource("/sql/simple_event.json");
    var arg = "select * from records where name like 'rick%';";
    testSqlProcess(inputEvent, arg, (RecordFleakData) null);
  }

  @Test
  public void testStringILikeMatch() throws IOException {
    var inputEvent = (RecordFleakData) loadFleakDataFromJsonResource("/sql/simple_event.json");
    var arg = "select * from records where name ilike 'rick%';";
    testSqlProcess(inputEvent, arg, inputEvent);
  }

  @Test
  public void testJsonCast() throws IOException {
    var recordFleakData =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/stringyfied_json_event.json");
    var arg = "select stringified_json::json data from records;";

    testSqlProcess(
        recordFleakData,
        arg,
        List.of((RecordFleakData) FleakData.wrap(Map.of("data", Map.of("k", 100)))));
  }

  private void testSqlProcess(
      RecordFleakData fleakData, String arg, RecordFleakData fieldAndExpectedValues) {
    SQLEvalCommand sqlEvalCommand = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);

    sqlEvalCommand.parseAndValidateArg(Map.of("sql", arg));
    sqlEvalCommand.initialize(metricClientProvider);
    var context = sqlEvalCommand.getExecutionContext();
    ScalarCommand.ProcessResult result = sqlEvalCommand.process(List.of(fleakData), null, context);
    if (fieldAndExpectedValues == null) {
      assertTrue(result.getFailureEvents().isEmpty());
      assertTrue(result.getOutput().isEmpty());
      return;
    }
    RecordFleakData output = result.getOutput().get(0);
    fieldAndExpectedValues
        .getPayload()
        .forEach(
            (f, expected) -> {
              FleakData actual = output.getPayload().get(f);
              System.out.println(actual);
              assertEquals(expected, actual);
            });
    verify(inputMessageCounter).increase(1L, Map.of());
    verify(outputMessageCounter).increase(1L, Map.of());
    verify(errorCounter, never()).increase(any());
  }

  private void testSqlProcess(
      RecordFleakData fleakData, String arg, List<RecordFleakData> fieldAndExpectedValues) {
    SQLEvalCommand sqlEvalCommand = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);

    sqlEvalCommand.parseAndValidateArg(Map.of("sql", arg));
    sqlEvalCommand.initialize(metricClientProvider);
    var context = sqlEvalCommand.getExecutionContext();
    ScalarCommand.ProcessResult result = sqlEvalCommand.process(List.of(fleakData), null, context);
    List<RecordFleakData> outputEvents = result.getOutput();
    System.out.println("outputEvents:");
    System.out.println(outputEvents);
    for (var failure : result.getFailureEvents()) {
      System.out.println(failure.errorMessage());
    }
    assertEquals(0, result.getFailureEvents().size());
    assertEquals(fieldAndExpectedValues.size(), outputEvents.size());

    int i = 0;
    for (RecordFleakData fieldAndExpectedValue : fieldAndExpectedValues) {
      RecordFleakData output = outputEvents.get(i++);
      fieldAndExpectedValue
          .getPayload()
          .forEach(
              (f, expected) -> {
                FleakData actual = output.getPayload().get(f);
                System.out.println(actual);
                assertEquals(expected, actual);
              });
    }
  }

  @Test
  public void testProcessMultiple() throws IOException {
    RecordFleakData event1 =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_json_get.json");
    RecordFleakData event2 =
        (RecordFleakData) loadFleakDataFromJsonResource("/sql/event_json_get.json");
    String arg = "select count(*) as cnt from records;";
    SQLEvalCommand command = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);
    command.parseAndValidateArg(Map.of("sql", arg));
    command.initialize(metricClientProvider);
    var context = command.getExecutionContext();
    ScalarCommand.ProcessResult output = command.process(List.of(event1, event2), null, context);
    assertTrue(output.getFailureEvents().isEmpty());
    assertEquals(1, output.getOutput().size());
    assertEquals(Map.of("cnt", 2L), output.getOutput().get(0).unwrap());
    verify(inputMessageCounter).increase(2, Map.of());
    verify(outputMessageCounter).increase(1, Map.of());
  }

  @Test
  public void testEventsAliasBackwardCompatibility() {
    RecordFleakData inputEvent =
        (RecordFleakData) FleakData.wrap(Map.of("name", "test", "value", 42));
    String arg = "select name, value from events";
    SQLEvalCommand command = new SqlCommandFactory().createCommand("myNodeId", JOB_CONTEXT);
    command.parseAndValidateArg(Map.of("sql", arg));
    command.initialize(metricClientProvider);
    var context = command.getExecutionContext();
    ScalarCommand.ProcessResult result = command.process(List.of(inputEvent), null, context);
    assertTrue(result.getFailureEvents().isEmpty(), "Should have no failures");
    assertEquals(1, result.getOutput().size(), "Should have 1 output");
    RecordFleakData output = result.getOutput().get(0);
    assertEquals("test", output.getPayload().get("name").unwrap());
    assertEquals(42L, output.getPayload().get("value").unwrap());
  }
}
