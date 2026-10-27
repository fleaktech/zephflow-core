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
package io.fleak.zephflow.lib.serdes.decode.otlp;

import io.fleak.zephflow.lib.serdes.decode.config.OtlpFormatConfig.Signal;
import java.util.List;
import java.util.Map;

/**
 * The records of an OTLP/JSON export request, one per log record, span or metric data point, with
 * the resource and scope on each. The {@code otlp} format produces the same records from protobuf.
 */
public final class OtlpRecords {

  private OtlpRecords() {}

  public static List<Map<String, Object>> fromJson(Signal signal, Map<String, Object> payload) {
    return switch (signal) {
      case LOGS -> OtlpLogRecords.flatten(payload);
      case METRICS -> OtlpMetricRecords.flatten(payload);
      case TRACES -> OtlpSpanRecords.flatten(payload);
    };
  }
}
