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
  @JsonSubTypes.Type(value = FramingConfig.Line.class, name = "line"),
  @JsonSubTypes.Type(value = FramingConfig.Multiline.class, name = "multiline"),
  @JsonSubTypes.Type(value = FramingConfig.Whole.class, name = "whole"),
})
public sealed interface FramingConfig {

  /** One record per line (LF, CRLF or CR). {@code skipBlank} defaults to true. */
  record Line(Boolean skipBlank) implements FramingConfig {}

  /**
   * Several lines become one record. Exactly one pattern is set: a line matching {@code
   * startPattern} starts a new event, or a line matching {@code continuePattern} continues the
   * current one. {@code maxLines} is unlimited when absent.
   */
  record Multiline(String startPattern, String continuePattern, Integer maxLines)
      implements FramingConfig {}

  /** The whole input is one record. */
  record Whole() implements FramingConfig {}
}
