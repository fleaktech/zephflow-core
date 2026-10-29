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
package io.fleak.zephflow.lib.serdes.decode.otlp;

import com.google.protobuf.Message;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.config.OtlpFormatConfig.Signal;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.Previews;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import io.fleak.zephflow.lib.serdes.decode.protobuf.ObjectBudget;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The whole input is one OTLP export request in binary protobuf; each log record, span or metric
 * data point is a record. Records are built one at a time as they are pulled.
 */
public final class OtlpFormat implements FormatCursor {

  private static final Set<String> METRIC_DATA =
      Set.of("gauge", "sum", "histogram", "exponentialHistogram", "summary");

  private record Leaf(Message message, Supplier<Map<String, Object>> record) {}

  private final InputStream in;
  private final DecodeLimits limits;
  private final Signal signal;
  private Iterator<Leaf> leaves;
  private long recordIndex;

  public OtlpFormat(InputStream in, DecodeLimits limits, Signal signal) {
    this.in = in;
    this.limits = limits;
    this.signal = signal;
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (leaves == null) {
      leaves = Collections.emptyIterator();
      switch (WholeInputReader.read(in, limits.maxDocumentBytes())) {
        case WholeInputReader.TooLarge tooLarge -> {
          return tooLarge.outcome();
        }
        case WholeInputReader.Whole whole -> {
          try {
            leaves = leaves(whole.bytes());
          } catch (IOException | RuntimeException e) {
            byte[] bytes = whole.bytes();
            return new Failed(
                ErrorKind.MALFORMED, bytes, -1, new RawSpan(0, bytes.length), e, true);
          }
        }
      }
    }
    if (!leaves.hasNext()) {
      return null;
    }
    Leaf leaf = leaves.next();
    recordIndex++;
    long size = leaf.message().getSerializedSize();
    long maxRecordBytes = limits.maxRecordBytes();
    if (maxRecordBytes > 0 && size > maxRecordBytes) {
      byte[] preview =
          leaf.message()
              .toByteString()
              .substring(0, (int) Math.min(size, Previews.PREVIEW_BYTES))
              .toByteArray();
      return new Oversized(
          recordIndex, 1, null, LimitKind.MAX_RECORD_BYTES, maxRecordBytes, size, preview, false);
    }
    return new Decoded((RecordFleakData) FleakData.wrap(leaf.record().get()), recordIndex, null);
  }

  private Iterator<Leaf> leaves(byte[] bytes) throws IOException {
    return switch (signal) {
      case LOGS -> {
        ObjectBudget.check(ExportLogsServiceRequest.getDescriptor(), bytes);
        yield logs(ExportLogsServiceRequest.parseFrom(bytes)).iterator();
      }
      case METRICS -> {
        ObjectBudget.check(ExportMetricsServiceRequest.getDescriptor(), bytes);
        yield metrics(ExportMetricsServiceRequest.parseFrom(bytes)).iterator();
      }
      case TRACES -> {
        ObjectBudget.check(ExportTraceServiceRequest.getDescriptor(), bytes);
        yield spans(ExportTraceServiceRequest.parseFrom(bytes)).iterator();
      }
    };
  }

  private static Stream<Leaf> logs(ExportLogsServiceRequest request) {
    return request.getResourceLogsList().stream()
        .flatMap(
            resourceLogs -> {
              Map<String, Object> resource = OtlpJsonMaps.of(resourceLogs, Set.of("scopeLogs"));
              return resourceLogs.getScopeLogsList().stream()
                  .flatMap(
                      scopeLogs -> {
                        OtlpEnvelope envelope =
                            OtlpEnvelope.of(
                                resource, OtlpJsonMaps.of(scopeLogs, Set.of("logRecords")));
                        return scopeLogs.getLogRecordsList().stream()
                            .map(
                                logRecord ->
                                    new Leaf(
                                        logRecord,
                                        () ->
                                            OtlpLogRecords.toRecord(
                                                OtlpJsonMaps.of(logRecord), envelope)));
                      });
            });
  }

  private static Stream<Leaf> spans(ExportTraceServiceRequest request) {
    return request.getResourceSpansList().stream()
        .flatMap(
            resourceSpans -> {
              Map<String, Object> resource = OtlpJsonMaps.of(resourceSpans, Set.of("scopeSpans"));
              return resourceSpans.getScopeSpansList().stream()
                  .flatMap(
                      scopeSpans -> {
                        OtlpEnvelope envelope =
                            OtlpEnvelope.of(resource, OtlpJsonMaps.of(scopeSpans, Set.of("spans")));
                        return scopeSpans.getSpansList().stream()
                            .map(
                                span ->
                                    new Leaf(
                                        span,
                                        () ->
                                            OtlpSpanRecords.toRecord(
                                                OtlpJsonMaps.of(span), envelope)));
                      });
            });
  }

  private static Stream<Leaf> metrics(ExportMetricsServiceRequest request) {
    return request.getResourceMetricsList().stream()
        .flatMap(
            resourceMetrics -> {
              Map<String, Object> resource =
                  OtlpJsonMaps.of(resourceMetrics, Set.of("scopeMetrics"));
              return resourceMetrics.getScopeMetricsList().stream()
                  .flatMap(
                      scopeMetrics -> {
                        OtlpEnvelope envelope =
                            OtlpEnvelope.of(
                                resource, OtlpJsonMaps.of(scopeMetrics, Set.of("metrics")));
                        return scopeMetrics.getMetricsList().stream()
                            .flatMap(metric -> points(metric, envelope));
                      });
            });
  }

  private static Stream<Leaf> points(Metric metric, OtlpEnvelope envelope) {
    Message data =
        switch (metric.getDataCase()) {
          case GAUGE -> metric.getGauge();
          case SUM -> metric.getSum();
          case HISTOGRAM -> metric.getHistogram();
          case EXPONENTIAL_HISTOGRAM -> metric.getExponentialHistogram();
          case SUMMARY -> metric.getSummary();
          case DATA_NOT_SET -> null;
        };
    if (data == null) {
      return Stream.empty();
    }
    Map<String, Object> metricMap = OtlpJsonMaps.of(metric, METRIC_DATA);
    String metricType =
        metric
            .getDescriptorForType()
            .findFieldByNumber(metric.getDataCase().getNumber())
            .getJsonName();
    Map<String, Object> dataMap = OtlpJsonMaps.of(data, Set.of("dataPoints"));
    @SuppressWarnings("unchecked")
    List<Message> points =
        (List<Message>) data.getField(data.getDescriptorForType().findFieldByName("data_points"));
    return points.stream()
        .map(
            point ->
                new Leaf(
                    point,
                    () ->
                        OtlpMetricRecords.point(
                            metricType, metricMap, dataMap, OtlpJsonMaps.of(point), envelope)));
  }

  @Override
  public long pendingRecordIndex() {
    return recordIndex + 1;
  }

  @Override
  public long pendingOffset() {
    return -1;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
