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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_HOST;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_INDEX;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_SOURCE;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_SOURCETYPE;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_TS;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.EncodingCompiler;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.encode.RecordEncoder;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public class SplunkHecSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<SplunkHecOutboundEvent> {

  @FunctionalInterface
  interface LineEncoder {
    String encode(RecordFleakData event) throws Exception;
  }

  private static final List<String> LABEL_FIELDS =
      List.of(FIELD_NAME_HOST, FIELD_NAME_SOURCE, FIELD_NAME_SOURCETYPE, FIELD_NAME_INDEX);
  private static final List<String> LABEL_AND_TIME_FIELDS =
      Stream.concat(LABEL_FIELDS.stream(), Stream.of(FIELD_NAME_TS)).toList();

  private final String index;
  private final String sourcetype;
  private final String source;
  private final LineEncoder lineEncoder;

  public SplunkHecSinkMessageProcessor(String index, String sourcetype, String source) {
    this(index, sourcetype, source, null);
  }

  private SplunkHecSinkMessageProcessor(
      String index, String sourcetype, String source, LineEncoder lineEncoder) {
    this.index = index;
    this.sourcetype = sourcetype;
    this.source = source;
    this.lineEncoder = lineEncoder;
  }

  static SplunkHecSinkMessageProcessor fromConfig(SplunkHecSinkDto.Config config) {
    LineEncoder lineEncoder =
        config.getEncoding() != null ? lineEncoder(config.getEncoding()) : null;
    return new SplunkHecSinkMessageProcessor(
        config.getIndex(), config.getSourcetype(), config.getSource(), lineEncoder);
  }

  private static LineEncoder lineEncoder(EncodingConfig encoding) {
    RecordEncoder encoder =
        EncodingCompiler.recordEncoder(encoding, SplunkHecSinkConfigValidator.CAPABILITY);
    return switch (encoding.format()) {
      case JsonFormatConfig json -> null;
      case TextFormatConfig text ->
          event -> new String(encoder.encode(event), StandardCharsets.UTF_8);
      default -> line(encoder);
    };
  }

  private static LineEncoder line(RecordEncoder encoder) {
    return event ->
        StringUtils.removeEnd(new String(encoder.encode(event), StandardCharsets.UTF_8), "\r\n");
  }

  @Override
  public SplunkHecOutboundEvent preprocess(RecordFleakData event, long ts) throws Exception {
    Map<String, FleakData> payload = event.getPayload();
    String host = label(payload, FIELD_NAME_HOST, null);
    String eventSourcetype = label(payload, FIELD_NAME_SOURCETYPE, sourcetype);
    String eventIndex = label(payload, FIELD_NAME_INDEX, index);
    String eventSource = label(payload, FIELD_NAME_SOURCE, source);
    BigDecimal time = time(payload.get(FIELD_NAME_TS));
    RecordFleakData body = withoutLabels(event, time != null);

    ObjectNode envelope = OBJECT_MAPPER.createObjectNode();
    if (lineEncoder == null) {
      envelope.set("event", OBJECT_MAPPER.readTree(toJsonString(body)));
    } else {
      envelope.put("event", lineEncoder.encode(body));
    }
    putIfPresent(envelope, "host", host);
    putIfPresent(envelope, "sourcetype", eventSourcetype);
    putIfPresent(envelope, "index", eventIndex);
    putIfPresent(envelope, "source", eventSource);
    if (time != null) {
      envelope.put("time", time);
    }
    String json = OBJECT_MAPPER.writeValueAsString(envelope) + "\n";
    return new SplunkHecOutboundEvent(json.getBytes(StandardCharsets.UTF_8));
  }

  static boolean setsIndex(RecordFleakData event) {
    return label(event.getPayload(), FIELD_NAME_INDEX, null) != null;
  }

  private static String label(Map<String, FleakData> payload, String field, String fallback) {
    if (payload.get(field) instanceof StringPrimitiveFleakData s
        && StringUtils.isNotBlank(s.getStringValue())) {
      return s.getStringValue();
    }
    return fallback;
  }

  private static BigDecimal time(FleakData ts) {
    if (!(ts instanceof NumberPrimitiveFleakData number)) {
      return null;
    }
    return BigDecimal.valueOf((long) Math.floor(number.getNumberValue()), 3);
  }

  private static void putIfPresent(ObjectNode envelope, String key, String value) {
    if (StringUtils.isNotBlank(value)) {
      envelope.put(key, value);
    }
  }

  private static RecordFleakData withoutLabels(RecordFleakData event, boolean dropTs) {
    Map<String, FleakData> payload = event.getPayload();
    List<String> sent = dropTs ? LABEL_AND_TIME_FIELDS : LABEL_FIELDS;
    if (sent.stream().noneMatch(payload::containsKey)) {
      return event;
    }
    Map<String, FleakData> body = new HashMap<>(payload);
    sent.forEach(body::remove);
    return new RecordFleakData(body);
  }
}
