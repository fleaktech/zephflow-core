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
package io.fleak.zephflow.lib.commands.s3filereader;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * The node on the generic decoder, against today's per-line legacy path: each case pins both the
 * legacy result and the node's, so a listed difference (spec 4.1 item 2) stays a difference and
 * every other line stays the same.
 */
class S3FileReaderDecodingTest {

  private static final String PATH = "s3://bucket/key";
  private static final Outcome FAILS = new Outcome(null);

  /** The records of an object compared as unwrapped maps, or null when the input record fails. */
  record Outcome(List<Map<String, Object>> records) {}

  private static Outcome records(Map<?, ?>... records) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Map<?, ?> record : records) {
      list.add(new HashMap<>((Map<String, Object>) record));
    }
    return new Outcome(list);
  }

  private static Map<String, Object> raw(String value) {
    return Map.of("__raw__", value);
  }

  static Stream<Arguments> cases() {
    String longString = "x".repeat(20_000_001);
    String longName = "k".repeat(50_001);
    String attributes =
        IntStream.range(0, 1022).mapToObj(i -> "a" + i + "=\"1\"").collect(Collectors.joining(" "));
    String cells =
        IntStream.range(0, 16_385).mapToObj(i -> "c" + i).collect(Collectors.joining(","));
    return Stream.of(
        // same on both paths
        same(
            EncodingType.JSON_OBJECT_LINE,
            "{\"a\":1}\n\n{\"b\":\"x\",\"c\":1.5}\n",
            records(Map.of("a", 1L), Map.of("b", "x", "c", 1.5))),
        same(EncodingType.JSON_OBJECT_LINE, " \t", records()),
        same(
            EncodingType.JSON_OBJECT,
            "{\"a\":1}\n{\"b\":2}",
            records(Map.of("a", 1L), Map.of("b", 2L))),
        same(
            EncodingType.JSON_ARRAY,
            "[{\"a\":1},{\"a\":2}]",
            records(Map.of("a", 1L), Map.of("a", 2L))),
        same(EncodingType.STRING_LINE, "a\n\nb", records(raw("a"), raw("b"))),
        same(EncodingType.TEXT, "a\n\nb", records(raw("a"), raw(""), raw("b"))),
        same(EncodingType.XML, "<r><a>1</a></r>", records(Map.of("a", "1"))),
        same(EncodingType.CSV, "a,b\nc,d\n a,b ", records()),
        same(EncodingType.CSV, "a,\"b", FAILS),
        same(EncodingType.CSV, "1,1,", FAILS),
        same(EncodingType.CSV, ",_c0", FAILS),
        // JSON_OBJECT_LINE
        diff(
            EncodingType.JSON_OBJECT_LINE,
            "[{\"a\":1},{\"a\":2}]",
            records(Map.of("a", 1L), Map.of("a", 2L)),
            FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "[]", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, " [ ] ", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "\u000B", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "\f", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "\u001C", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "\u2028", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "\u3000", records(), FAILS),
        diff(EncodingType.JSON_OBJECT_LINE, "\uFEFF{\"a\":1}", FAILS, records(Map.of("a", 1L))),
        diff(
            EncodingType.JSON_OBJECT_LINE,
            "{\"a\":1}\n\uFEFF{\"b\":2}",
            FAILS,
            records(Map.of("a", 1L), Map.of("b", 2L))),
        diff(EncodingType.JSON_OBJECT_LINE, "\uFEFF", FAILS, records()),
        // byte-order mark, text formats
        diff(EncodingType.STRING_LINE, "\uFEFFab", records(raw("\uFEFFab")), records(raw("ab"))),
        diff(
            EncodingType.STRING_LINE,
            "a\n\uFEFF",
            records(raw("a"), raw("\uFEFF")),
            records(raw("a"))),
        diff(EncodingType.TEXT, "\uFEFFab", records(raw("\uFEFFab")), records(raw("ab"))),
        diff(EncodingType.TEXT, "\uFEFF", records(raw("\uFEFF")), records(raw(""))),
        // empty line
        diff(
            EncodingType.JSON_OBJECT,
            "{\"a\":1}\n\n{\"b\":2}",
            FAILS,
            records(Map.of("a", 1L), Map.of("b", 2L))),
        diff(EncodingType.JSON_ARRAY, "[{\"a\":1}]\n\n", FAILS, records(Map.of("a", 1L))),
        diff(EncodingType.XML, "<r><a>1</a></r>\n\n", FAILS, records(Map.of("a", "1"))),
        // size limits
        diff(
            EncodingType.JSON_OBJECT_LINE,
            "{\"a\":\"" + longString + "\"}",
            FAILS,
            records(Map.of("a", longString))),
        diff(
            EncodingType.JSON_OBJECT_LINE,
            "{\"" + longName + "\":1}",
            FAILS,
            records(Map.of(longName, 1L))),
        diff(
            EncodingType.JSON_OBJECT,
            "{\"a\":\"" + longString + "\"}",
            FAILS,
            records(Map.of("a", longString))),
        diff(
            EncodingType.JSON_OBJECT,
            "{\"" + longName + "\":1}",
            FAILS,
            records(Map.of(longName, 1L))),
        diff(
            EncodingType.JSON_ARRAY,
            "[{\"a\":\"" + longString + "\"}]",
            FAILS,
            records(Map.of("a", longString))),
        diff(
            EncodingType.JSON_ARRAY,
            "[{\"" + longName + "\":1}]",
            FAILS,
            records(Map.of(longName, 1L))),
        diff(
            EncodingType.XML,
            "<r " + attributes + "/>",
            FAILS,
            records(
                IntStream.range(0, 1022)
                    .boxed()
                    .collect(Collectors.toMap(i -> "a" + i, i -> "1")))),
        same(
            EncodingType.XML,
            "<r a=\"" + "v".repeat(800_000) + "\"/>",
            records(Map.of("a", "v".repeat(800_000)))),
        diff(
            EncodingType.XML,
            "<r a=\"" + "v".repeat(900_000) + "\"/>",
            FAILS,
            records(Map.of("a", "v".repeat(900_000)))),
        // XML documents
        diff(EncodingType.XML, "<r>1</r>x", records(Map.of("", "1")), FAILS),
        diff(EncodingType.XML, "<r/><s/>", records(Map.of()), FAILS),
        diff(EncodingType.XML, "<!DOCTYPE r><r>1</r>", records(Map.of("", "1")), FAILS),
        // CSV
        same(EncodingType.CSV, "a,b\n\nc,d", records()),
        same(EncodingType.CSV, "\"　\"", records()),
        same(EncodingType.CSV, "\" a\",a", records()),
        same(EncodingType.CSV, "\"a\"x", FAILS),
        diff(EncodingType.CSV, "　\"a\"x", FAILS, records()),
        diff(EncodingType.CSV, "a,\u000C\"b", FAILS, records()),
        diff(EncodingType.CSV, "\"a\"　", records(), FAILS),
        diff(EncodingType.CSV, "\"a\"\u001C,b", records(), FAILS),
        diff(EncodingType.CSV, "\" \"", FAILS, records()),
        diff(EncodingType.CSV, "\t", FAILS, records()),
        diff(EncodingType.CSV, "\"a\" ,a", records(), FAILS),
        diff(EncodingType.CSV, "1,1", records(), FAILS),
        diff(EncodingType.CSV, " a,a ", records(), FAILS),
        diff(EncodingType.CSV, "a,a\t", records(), FAILS),
        diff(EncodingType.CSV, "\uFEFFa,a", records(), FAILS),
        diff(EncodingType.CSV, "a,,b", FAILS, records()),
        diff(EncodingType.CSV, "a, ,b", FAILS, records()),
        diff(EncodingType.CSV, "a,\u0001,b", FAILS, records()),
        diff(EncodingType.CSV, "a,\u3000\u0001", FAILS, records()),
        diff(EncodingType.CSV, "a,\u2028\u0001\u2028", FAILS, records()),
        same(EncodingType.CSV, "\u0085", records()),
        same(EncodingType.CSV, "a,\u007F", records()),
        diff(EncodingType.CSV, ",a", FAILS, records()),
        diff(EncodingType.CSV, "a,b,", FAILS, records()),
        diff(EncodingType.CSV, "\"\"", FAILS, records()),
        diff(EncodingType.CSV, " ", FAILS, records()),
        diff(EncodingType.CSV, "\u000B", FAILS, records()),
        diff(EncodingType.CSV, "\u0000", FAILS, records()),
        diff(EncodingType.CSV, "\u2028", FAILS, records()),
        diff(EncodingType.CSV, cells, records(), FAILS));
  }

  private static Arguments same(EncodingType encodingType, String object, Outcome outcome) {
    return Arguments.of(encodingType, object, outcome, outcome);
  }

  private static Arguments diff(
      EncodingType encodingType, String object, Outcome today, Outcome now) {
    return Arguments.of(encodingType, object, today, now);
  }

  @ParameterizedTest
  @MethodSource("cases")
  void matchesTheLegacyPathExceptTheListedDifferences(
      EncodingType encodingType, String object, Outcome today, Outcome now) throws Exception {
    byte[] bytes = object.getBytes(StandardCharsets.UTF_8);

    assertEquals(today, legacy(encodingType, bytes));
    assertEquals(now, outcome(run(config(encodingType), bytes, PATH)));
  }

  @Test
  void badLineFailsTheInputRecordWithTheDecodeText() throws Exception {
    FleakCounter errorCounter = mock();
    MetricClientProvider metrics = mock();
    when(metrics.counter(any(), anyMap())).thenAnswer(i -> mock(FleakCounter.class));
    when(metrics.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), anyMap())).thenReturn(errorCounter);
    String message =
        """
        MALFORMED json line: JsonParseException
        Action: check that each line of the object is what the node's encodingType \
        JSON_OBJECT_LINE expects, or fix the object; the input record failed and no record of \
        this object was emitted
        Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token \
        'null', 'true' or 'false')
        s3://bucket/key line 3, recordIndex 1, bytes 0-8""";

    ScalarCommand.ProcessResult result;
    List<Logged> logged;
    try (BoundedLogCapture logs = new BoundedLogCapture(S3FileReaderCommand.class)) {
      result =
          run(
              config(EncodingType.JSON_OBJECT_LINE),
              "{\"a\":1}\n\nnot json".getBytes(StandardCharsets.UTF_8),
              PATH,
              metrics);
      logged =
          logs.events().stream()
              .filter(e -> e.getLevel().isMoreSpecificThan(Level.WARN))
              .map(Logged::of)
              .toList();
    }

    assertEquals(List.of(), result.getOutput());
    assertEquals(List.of(new ErrorOutput(input(PATH), message)), result.getFailureEvents());
    verify(errorCounter).increase(anyMap());
    assertEquals(List.of(new Logged(Level.WARN, message, JsonParseException.class)), logged);
  }

  private record Logged(Level level, String message, Class<?> thrown) {
    static Logged of(LogEvent event) {
      return new Logged(
          event.getLevel(),
          event.getMessage().getFormattedMessage(),
          event.getThrown() == null ? null : event.getThrown().getClass());
    }
  }

  @Test
  void csvFailureLocationEndsAtTheLine() throws Exception {
    ScalarCommand.ProcessResult result =
        run(config(EncodingType.CSV), "a,b\n1,1".getBytes(StandardCharsets.UTF_8), PATH);

    assertEquals(
        List.of(
            new ErrorOutput(
                input(PATH),
                """
                MALFORMED csv line: MalformedCsvException
                Action: check that each line of the object is what the node's encodingType CSV \
                expects, or fix the object; the input record failed and no record of this object \
                was emitted
                header has a duplicate column name: 1
                s3://bucket/key line 2""")),
        result.getFailureEvents());
  }

  @Test
  void decoderThatThrowsGivesReadFailure() throws Exception {
    RecordDecoder decoder = mock();
    when(decoder.decodeAll(any(), any()))
        .thenThrow(new UncheckedIOException(new IOException("disk gone\nsecond line")));
    ScalarCommand.ProcessResult result;
    try (MockedStatic<DecodingConfigCompiler> compiler = mockStatic(DecodingConfigCompiler.class)) {
      compiler.when(() -> DecodingConfigCompiler.compile(any())).thenReturn(decoder);
      result =
          run(config(EncodingType.JSON_OBJECT_LINE), "{}".getBytes(StandardCharsets.UTF_8), PATH);
    }

    assertEquals(
        List.of(
            new ErrorOutput(
                input(PATH),
                """
                READ_FAILURE json line: IOException
                Action: check that each line of the object is what the node's encodingType \
                JSON_OBJECT_LINE expects, or fix the object; the input record failed and no record \
                of this object was emitted
                disk gone
                s3://bucket/key line 1""")),
        result.getFailureEvents());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", " \nsecond line"})
  void causeWithoutMessageIsNamedByItsClass(String causeMessage) throws Exception {
    RecordDecoder decoder = mock();
    when(decoder.decodeAll(any(), any())).thenThrow(new IllegalStateException(causeMessage));
    ScalarCommand.ProcessResult result;
    try (MockedStatic<DecodingConfigCompiler> compiler = mockStatic(DecodingConfigCompiler.class)) {
      compiler.when(() -> DecodingConfigCompiler.compile(any())).thenReturn(decoder);
      result =
          run(config(EncodingType.JSON_OBJECT_LINE), "{}".getBytes(StandardCharsets.UTF_8), PATH);
    }

    assertEquals(
        List.of(
            new ErrorOutput(
                input(PATH),
                """
                READ_FAILURE json line: IllegalStateException
                Action: check that each line of the object is what the node's encodingType \
                JSON_OBJECT_LINE expects, or fix the object; the input record failed and no record \
                of this object was emitted
                IllegalStateException
                s3://bucket/key line 1""")),
        result.getFailureEvents());
  }

  @Test
  void gunzipsByCompression() throws Exception {
    byte[] plain = "{\"a\":1}\n{\"a\":2}".getBytes(StandardCharsets.UTF_8);
    List<RecordFleakData> expected = List.of(record(Map.of("a", 1L)), record(Map.of("a", 2L)));
    Map<String, Object> auto = config(EncodingType.JSON_OBJECT_LINE);
    Map<String, Object> gzip = config(EncodingType.JSON_OBJECT_LINE);
    gzip.put("compression", "GZIP");
    Map<String, Object> none = config(EncodingType.JSON_OBJECT_LINE);
    none.put("compression", "NONE");

    assertEquals(expected, run(auto, gzip(plain), "s3://bucket/key.gz").getOutput());
    assertEquals(expected, run(gzip, gzip(plain), "s3://bucket/key").getOutput());
    assertEquals(expected, run(none, plain, "s3://bucket/key.gz").getOutput());
  }

  @Test
  void lineEmissionIsUnchanged() throws Exception {
    Map<String, Object> config = config(EncodingType.JSON_OBJECT_LINE);
    config.put("emission", "LINE");

    assertEquals(
        List.of(
            record(Map.of("line", "not json", "file", PATH)),
            record(Map.of("line", "", "file", PATH))),
        run(config, "not json\n\n".getBytes(StandardCharsets.UTF_8), PATH).getOutput());
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"PARQUET", "KV"})
  void sinkOnlyFormatsFailAtStart(EncodingType encodingType) {
    try (MockedStatic<S3Backend> backend = mockStatic(S3Backend.class)) {
      backend.when(() -> S3Backend.client(any())).thenReturn(mock(S3Client.class));
      OperatorCommand command = command(config(encodingType));

      UnsupportedOperationException e =
          assertThrows(
              UnsupportedOperationException.class,
              () -> command.initialize(new MetricClientProvider.NoopMetricClientProvider()));
      assertEquals(
          encodingType + " is a sink-only format (not supported for deserialization)",
          e.getMessage());
    }
  }

  /** Today's node: the legacy deserializer on each line the object's UTF-8 reader returns. */
  private static Outcome legacy(EncodingType encodingType, byte[] bytes) throws IOException {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();
    List<RecordFleakData> records = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        records.addAll(
            deserializer.deserialize(
                new SerializedEvent(null, line.getBytes(StandardCharsets.UTF_8), null)));
      }
    } catch (Exception e) {
      return FAILS;
    }
    return outcome(records);
  }

  private static Outcome outcome(ScalarCommand.ProcessResult result) {
    return result.getFailureEvents().isEmpty() ? outcome(result.getOutput()) : FAILS;
  }

  private static Outcome outcome(List<RecordFleakData> records) {
    return new Outcome(records.stream().map(RecordFleakData::unwrap).toList());
  }

  private static Map<String, Object> config(EncodingType encodingType) {
    Map<String, Object> config = new HashMap<>();
    config.put("pathField", "s3Path");
    config.put("region", "us-east-1");
    config.put("emission", "DESERIALIZE");
    config.put("encodingType", encodingType.name());
    return config;
  }

  private static ScalarCommand.ProcessResult run(
      Map<String, Object> config, byte[] object, String path) {
    return run(config, object, path, new MetricClientProvider.NoopMetricClientProvider());
  }

  private static ScalarCommand.ProcessResult run(
      Map<String, Object> config, byte[] object, String path, MetricClientProvider metrics) {
    S3Client client = mock();
    when(client.getObject(any(GetObjectRequest.class)))
        .thenAnswer(
            i ->
                new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    AbortableInputStream.create(new ByteArrayInputStream(object))));
    try (MockedStatic<S3Backend> backend = mockStatic(S3Backend.class)) {
      backend.when(() -> S3Backend.client(any())).thenReturn(client);
      OperatorCommand command = command(config);
      command.initialize(metrics);
      return ((ScalarCommand) command)
          .process(List.of(input(path)), "test_user", command.getExecutionContext());
    }
  }

  private static OperatorCommand command(Map<String, Object> config) {
    OperatorCommand command =
        new S3FileReaderCommandFactory().createCommand("node", TestUtils.buildJobContext(Map.of()));
    command.parseAndValidateArg(config);
    return command;
  }

  private static RecordFleakData input(String path) {
    return record(Map.of("s3Path", path));
  }

  private static RecordFleakData record(Map<String, Object> payload) {
    return (RecordFleakData) FleakData.wrap(payload);
  }

  private static byte[] gzip(byte[] bytes) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(bytes);
    }
    return out.toByteArray();
  }
}
