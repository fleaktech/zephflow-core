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
package io.fleak.zephflow.lib.serdes.encode;

import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import java.util.Set;

/**
 * What one sink can write, declared next to its config validator.
 *
 * @param sink the command name used in error messages
 * @param formats the format type names it writes, among csv, json, kv and parquet
 * @param jsonModes the json modes it writes
 * @param csvHeaderOffWithColumns csv only with header false and columns set
 * @param compression the codecs it accepts besides none
 */
public record WriterCapability(
    String sink,
    Unit unit,
    Set<String> formats,
    Set<JsonFormatConfig.Mode> jsonModes,
    boolean csvHeaderOffWithColumns,
    Set<DecompressionCodec> compression) {

  /** What the sink writes as one piece: a message, a queue entry, a HEC event, a file. */
  public enum Unit {
    ONE_RECORD,
    MANY_RECORDS
  }
}
