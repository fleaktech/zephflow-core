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
package io.fleak.zephflow.lib.serdes.decode.csv;

import java.nio.charset.Charset;
import java.util.List;

/**
 * A compiled CSV configuration. Single-byte options are ASCII byte values, -1 when off. {@code
 * columns} is null when names come from the header line or are generated.
 */
public record CsvDialect(
    int delimiter,
    int quote,
    int escape,
    int commentMarker,
    boolean ignoreSurroundingSpaces,
    int skipLines,
    boolean header,
    List<String> columns,
    Charset charset) {}
