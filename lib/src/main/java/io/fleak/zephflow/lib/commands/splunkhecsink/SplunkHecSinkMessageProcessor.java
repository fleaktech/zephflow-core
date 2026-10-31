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
import static io.fleak.zephflow.lib.utils.MiscUtils.parseEnum;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.EncodingCompiler;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.encode.RecordEncoder;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import io.fleak.zephflow.lib.serdes.ser.csv.CsvSerializerOptions;
import io.fleak.zephflow.lib.serdes.ser.kv.KvTypedSerializer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;

public class SplunkHecSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<SplunkHecOutboundEvent> {

  @FunctionalInterface
  interface LineEncoder {
    String encode(RecordFleakData event) throws Exception;
  }

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
        config.getEncoding() != null ? lineEncoder(config.getEncoding()) : lineEncoder(config);
    return new SplunkHecSinkMessageProcessor(
        config.getIndex(), config.getSourcetype(), config.getSource(), lineEncoder);
  }

  private static LineEncoder lineEncoder(EncodingConfig encoding) {
    RecordEncoder encoder =
        EncodingCompiler.recordEncoder(encoding, SplunkHecSinkConfigValidator.CAPABILITY);
    return encoding.format() instanceof JsonFormatConfig ? null : line(encoder);
  }

  private static LineEncoder lineEncoder(SplunkHecSinkDto.Config config) {
    EncodingType encodingType = encodingType(config);
    if (encodingType == EncodingType.KV) {
      KvTypedSerializer kvSerializer = new KvTypedSerializer(config.kvOptions());
      return event -> kvSerializer.toLine(event.unwrap());
    }
    if (encodingType == EncodingType.CSV) {
      FleakSerializer<?> csvSerializer =
          SerializerFactory.createSerializerFactory(EncodingType.CSV, csvOptions(config))
              .createSerializer();
      return line(SinkEncoding.fromSerializer(csvSerializer));
    }
    return null;
  }

  private static LineEncoder line(RecordEncoder encoder) {
    return event ->
        StringUtils.removeEnd(new String(encoder.encode(event), StandardCharsets.UTF_8), "\r\n");
  }

  static CsvSerializerOptions csvOptions(SplunkHecSinkDto.Config config) {
    if (encodingType(config) != EncodingType.CSV) {
      return null;
    }
    return new CsvSerializerOptions(config.getCsvColumns(), false);
  }

  private static EncodingType encodingType(SplunkHecSinkDto.Config config) {
    return StringUtils.isBlank(config.getEncodingType())
        ? null
        : parseEnum(EncodingType.class, config.getEncodingType());
  }

  @Override
  public SplunkHecOutboundEvent preprocess(RecordFleakData event, long ts) throws Exception {
    ObjectNode envelope = OBJECT_MAPPER.createObjectNode();
    if (lineEncoder == null) {
      envelope.set("event", OBJECT_MAPPER.readTree(toJsonString(event)));
    } else {
      envelope.put("event", lineEncoder.encode(event));
    }
    if (StringUtils.isNotBlank(sourcetype)) {
      envelope.put("sourcetype", sourcetype);
    }
    if (StringUtils.isNotBlank(index)) {
      envelope.put("index", index);
    }
    if (StringUtils.isNotBlank(source)) {
      envelope.put("source", source);
    }
    String json = OBJECT_MAPPER.writeValueAsString(envelope) + "\n";
    return new SplunkHecOutboundEvent(json.getBytes(StandardCharsets.UTF_8));
  }
}
