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
 * {@code key=value} pairs, one record per line. Written by sinks only: no source reads it. {@code
 * fields} picks and orders the keys (default: every non-null field in case-insensitive key order);
 * {@code pairDelimiter} separates the pairs (default one space); {@code cleanFields} replaces every
 * character of a key other than letters, digits and {@code _} with {@code _} (default false).
 */
public record KvFormatConfig(List<String> fields, String pairDelimiter, Boolean cleanFields)
    implements FormatConfig {}
