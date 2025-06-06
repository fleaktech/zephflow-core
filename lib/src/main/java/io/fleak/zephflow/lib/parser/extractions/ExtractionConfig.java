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
package io.fleak.zephflow.lib.parser.extractions;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = GrokExtractionConfig.class, name = "grok"),
  @JsonSubTypes.Type(value = WindowsMultilineExtractionConfig.class, name = "windows_multiline"),
  @JsonSubTypes.Type(value = SyslogExtractionConfig.class, name = "syslog"),
  @JsonSubTypes.Type(value = CefExtractionConfig.class, name = "cef"),
  @JsonSubTypes.Type(value = PanwTrafficExtractionConfig.class, name = "panw_traffic"),
  @JsonSubTypes.Type(value = JsonExtractionConfig.class, name = "json"),
  @JsonSubTypes.Type(value = DelimitedTextExtractionConfig.class, name = "delimited_text"),
})
public interface ExtractionConfig {}
