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

import java.util.List;

/**
 * Delimited text. Single-character options must be one ASCII character. {@code quote} defaults to
 * {@code "} and an empty string turns quoting off; {@code escape} and {@code commentMarker} are off
 * when absent. {@code header} (default true) says whether the first record holds column names;
 * {@code columns} names the columns explicitly and wins over the header. With neither, columns are
 * named {@code _c0}, {@code _c1}, …
 */
public record CsvFormatConfig(
    String delimiter,
    String quote,
    String escape,
    Boolean header,
    List<String> columns,
    Integer skipLines,
    String commentMarker,
    Boolean ignoreSurroundingSpaces,
    String charset)
    implements FormatConfig {}
