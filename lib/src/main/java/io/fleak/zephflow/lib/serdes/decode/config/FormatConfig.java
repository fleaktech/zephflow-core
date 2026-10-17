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
package io.fleak.zephflow.lib.serdes.decode.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = AvroFormatConfig.class, name = "avro"),
  @JsonSubTypes.Type(value = CsvFormatConfig.class, name = "csv"),
  @JsonSubTypes.Type(value = JsonFormatConfig.class, name = "json"),
  @JsonSubTypes.Type(value = ProtobufFormatConfig.class, name = "protobuf"),
  @JsonSubTypes.Type(value = TextFormatConfig.class, name = "text"),
  @JsonSubTypes.Type(value = XmlFormatConfig.class, name = "xml"),
  @JsonSubTypes.Type(value = YamlFormatConfig.class, name = "yaml"),
})
public sealed interface FormatConfig
    permits AvroFormatConfig,
        CsvFormatConfig,
        JsonFormatConfig,
        ProtobufFormatConfig,
        TextFormatConfig,
        XmlFormatConfig,
        YamlFormatConfig {}
