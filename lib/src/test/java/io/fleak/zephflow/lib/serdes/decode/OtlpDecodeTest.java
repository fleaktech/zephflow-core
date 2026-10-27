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
package io.fleak.zephflow.lib.serdes.decode;

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.decode;
import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.decoder;
import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.gzipOne;
import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures.map;
import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures.printedAsOtlpJson;
import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures.protobuf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Message;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.Bad;
import io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.Big;
import io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.Ok;
import io.fleak.zephflow.lib.serdes.decode.config.OtlpFormatConfig.Signal;
import io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFixtures;
import io.fleak.zephflow.lib.serdes.decode.otlp.OtlpRecords;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class OtlpDecodeTest {

  private static final String LOGS = config("logs");
  private static final String METRICS = config("metrics");
  private static final String TRACES = config("traces");

  private static String config(String signal) {
    return "{\"format\":{\"type\":\"otlp\",\"signal\":\"" + signal + "\"}}";
  }

  private static List<Object> decoded(List<Map<String, Object>> records) {
    return IntStream.range(0, records.size())
        .mapToObj(i -> (Object) new Ok(record(records.get(i)), i + 1, null))
        .toList();
  }

  private static RecordFleakData record(Map<String, Object> record) {
    return (RecordFleakData) FleakData.wrap(record);
  }

  private static ExportLogsServiceRequest logsFixture() throws Exception {
    return protobuf("otlp_logs.json", ExportLogsServiceRequest.newBuilder());
  }

  @Test
  void logsDecodeToOneRecordPerLogRecord() throws Exception {
    assertEquals(decoded(OtlpFixtures.logs()), decode(LOGS, logsFixture().toByteArray()));
  }

  @Test
  void metricsDecodeToOneRecordPerDataPoint() throws Exception {
    byte[] input =
        protobuf("otlp_metrics.json", ExportMetricsServiceRequest.newBuilder()).toByteArray();
    assertEquals(decoded(OtlpFixtures.metrics(false)), decode(METRICS, input));
  }

  @Test
  void tracesDecodeToOneRecordPerSpan() throws Exception {
    byte[] input =
        protobuf("otlp_traces.json", ExportTraceServiceRequest.newBuilder()).toByteArray();
    assertEquals(decoded(OtlpFixtures.traces()), decode(TRACES, input));
  }

  @Test
  void recordsEqualThoseOfTheSameMessageAsOtlpJson() throws Exception {
    List<Message> messages =
        List.of(
            logsFixture(),
            protobuf("otlp_metrics.json", ExportMetricsServiceRequest.newBuilder()),
            protobuf("otlp_traces.json", ExportTraceServiceRequest.newBuilder()));
    List<Signal> signals = List.of(Signal.LOGS, Signal.METRICS, Signal.TRACES);
    for (int i = 0; i < messages.size(); i++) {
      Message message = messages.get(i);
      Signal signal = signals.get(i);
      assertEquals(
          decoded(OtlpRecords.fromJson(signal, printedAsOtlpJson(message))),
          decode(config(signal.configName()), message.toByteArray()));
    }
  }

  @Test
  void compressedRequestDecodesLikeAPlainOne() throws Exception {
    assertEquals(
        decoded(OtlpFixtures.logs()),
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"otlp\",\"signal\":\"logs\"}}",
            gzipOne(logsFixture().toByteArray())));
  }

  @Test
  void emptyRequestHasNoRecords() {
    assertEquals(List.of(), decode(TRACES, new byte[0]));
  }

  @Test
  void metricWithoutDataIsSkipped() {
    ExportMetricsServiceRequest request =
        ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                ResourceMetrics.newBuilder()
                    .addScopeMetrics(
                        ScopeMetrics.newBuilder()
                            .addMetrics(Metric.newBuilder().setName("empty"))
                            .addMetrics(
                                Metric.newBuilder()
                                    .setName("depth")
                                    .setGauge(
                                        Gauge.newBuilder()
                                            .addDataPoints(
                                                NumberDataPoint.newBuilder().setAsInt(2))))))
            .build();
    assertEquals(
        decoded(List.of(map("metricType", "gauge", "name", "depth", "asInt", "2"))),
        decode(METRICS, request.toByteArray()));
  }

  @Test
  void recordOverTheRecordLimitIsOversizedAndTheOthersDecode() {
    LogRecord big = log("x".repeat(100));
    ExportLogsServiceRequest request =
        ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(
                ResourceLogs.newBuilder()
                    .addScopeLogs(
                        ScopeLogs.newBuilder()
                            .addLogRecords(log("a"))
                            .addLogRecords(big)
                            .addLogRecords(log("b"))))
            .build();
    assertEquals(
        List.of(
            new Ok(record(map("body", "a")), 1, null),
            new Big(
                2,
                1,
                null,
                LimitKind.MAX_RECORD_BYTES,
                50,
                big.getSerializedSize(),
                big.toByteString().toString(StandardCharsets.UTF_8),
                false),
            new Ok(record(map("body", "b")), 3, null)),
        decode(decoder(LOGS), ByteInput.ofBytes(request.toByteArray()), new DecodeLimits(50, 0)));
  }

  private static LogRecord log(String body) {
    return LogRecord.newBuilder().setBody(AnyValue.newBuilder().setStringValue(body)).build();
  }

  @Test
  void requestThatDoesNotParseIsTerminal() {
    byte[] input = {(byte) 0x0A, 5, 'a'};
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(input, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, 3),
                "InvalidProtocolBufferException: While parsing a protocol message, the input ended unexpectedly in the middle of a field.  This could mean either that the input has been truncated or that an embedded message misreported its own length.",
                true)),
        decode(LOGS, input));
  }

  @Test
  void requestOverTheDocumentLimitIsOversized() throws Exception {
    byte[] input = logsFixture().toByteArray();
    assertEquals(
        List.of(
            new Big(
                -1,
                1,
                RawSpan.toEnd(0),
                LimitKind.MAX_DOCUMENT_BYTES,
                10,
                input.length,
                new String(input, StandardCharsets.UTF_8),
                true)),
        decode(decoder(LOGS), ByteInput.ofBytes(input), new DecodeLimits(0, 10)));
  }

  @Test
  void signalIsRequiredAndMustBeKnown() {
    assertEquals(
        "otlp format needs a signal: logs, metrics or traces",
        assertThrows(
                IllegalArgumentException.class, () -> decoder("{\"format\":{\"type\":\"otlp\"}}"))
            .getMessage());
    assertThrows(RuntimeException.class, () -> decoder(config("profiles")));
  }
}
