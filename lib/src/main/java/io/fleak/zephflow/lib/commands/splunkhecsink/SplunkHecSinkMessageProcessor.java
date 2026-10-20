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
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import io.fleak.zephflow.lib.serdes.ser.csv.CsvSerializerOptions;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class SplunkHecSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<SplunkHecOutboundEvent> {

  private final String index;
  private final String sourcetype;
  private final String source;
  private final FleakSerializer<?> csvSerializer;

  public SplunkHecSinkMessageProcessor(String index, String sourcetype, String source) {
    this(index, sourcetype, source, null);
  }

  public SplunkHecSinkMessageProcessor(
      String index, String sourcetype, String source, CsvSerializerOptions csvOptions) {
    this.index = index;
    this.sourcetype = sourcetype;
    this.source = source;
    this.csvSerializer =
        csvOptions == null
            ? null
            : SerializerFactory.createSerializerFactory(EncodingType.CSV, csvOptions)
                .createSerializer();
  }

  static CsvSerializerOptions csvOptions(SplunkHecSinkDto.Config config) {
    if (StringUtils.isBlank(config.getEncodingType())
        || parseEnum(EncodingType.class, config.getEncodingType()) != EncodingType.CSV) {
      return null;
    }
    return new CsvSerializerOptions(config.getCsvColumns(), false);
  }

  @Override
  public SplunkHecOutboundEvent preprocess(RecordFleakData event, long ts) throws Exception {
    ObjectNode envelope = OBJECT_MAPPER.createObjectNode();
    if (csvSerializer == null) {
      envelope.set("event", OBJECT_MAPPER.readTree(toJsonString(event)));
    } else {
      envelope.put("event", toCsvLine(event));
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

  private String toCsvLine(RecordFleakData event) throws Exception {
    byte[] bytes = csvSerializer.serialize(List.of(event)).value();
    return StringUtils.removeEnd(new String(bytes, Charset.defaultCharset()), "\r\n");
  }
}
