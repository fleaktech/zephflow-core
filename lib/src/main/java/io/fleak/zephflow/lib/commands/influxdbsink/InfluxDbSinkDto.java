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
package io.fleak.zephflow.lib.commands.influxdbsink;

import com.influxdb.client.domain.WritePrecision;
import io.fleak.zephflow.api.CommandConfig;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for the InfluxDB (v2 write API) sink connector. */
public interface InfluxDbSinkDto {

  int DEFAULT_BATCH_SIZE = 1000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {

    // --- Connection / authentication ---

    /** InfluxDB base URL, e.g. {@code https://influx:8086}. */
    private String url;

    /** InfluxDB v2 organization. */
    private String org;

    /** InfluxDB v2 bucket to write to. */
    private String bucket;

    /**
     * Credential id resolving to an {@code ApiKeyCredential} whose key is the InfluxDB API token.
     * When unset, the sink connects without authentication (local / unsecured InfluxDB).
     */
    private String credentialId;

    // --- Mapping: record -> measurement / tags / fields / timestamp ---
    // All selectors are top-level record field names.

    /** Literal measurement name. Exactly one of {@code measurement} / {@code measurementField}. */
    private String measurement;

    /** Record field whose value provides the measurement name (dynamic routing). */
    private String measurementField;

    /** Record fields to write as tags (string-coerced). */
    @Builder.Default private List<String> tagFields = new ArrayList<>();

    /**
     * Record fields to write as InfluxDB fields, typed by value. When null or empty, all remaining
     * record fields (those not used as a tag, the timestamp, or the measurement source) are
     * written.
     */
    private List<String> fieldFields;

    /** Optional record field holding the point timestamp. When unset, write time is used. */
    private String timestampField;

    /** Timestamp precision: {@code NS}, {@code US}, {@code MS} (default), or {@code S}. */
    @Builder.Default private WritePrecision precision = WritePrecision.MS;

    /** Max records per write batch. */
    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
