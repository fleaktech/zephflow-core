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

import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpJson.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The resource/scope envelope hoisted onto each leaf record, shared because {@code ResourceLogs},
 * {@code ResourceMetrics} and {@code ResourceSpans} carry the identical shape.
 *
 * <p>Known loss: {@code Resource.droppedAttributesCount} has nowhere to go. The shipped logs record
 * flattens {@code resource} to just the attribute map, so housing the counter means reshaping to
 * {@code resource: {attributes, droppedAttributesCount}} — a breaking change to a live record shape
 * for a value that is essentially always zero.
 */
record OtlpEnvelope(
    Map<String, Object> resource,
    Map<String, Object> scope,
    String resourceSchemaUrl,
    String scopeSchemaUrl) {

  /**
   * @param resourceEntry a {@code resourceLogs}/{@code resourceMetrics}/{@code resourceSpans}
   *     element
   * @param scopeEntry the {@code scopeLogs}/{@code scopeMetrics}/{@code scopeSpans} element inside
   *     it
   */
  static OtlpEnvelope of(Map<String, Object> resourceEntry, Map<String, Object> scopeEntry) {
    return new OtlpEnvelope(
        attributes(field(resourceEntry.get("resource"), "attributes")),
        scope(map(scopeEntry.get("scope"))),
        // Stringified rather than cast: a ClassCastException here would dead-letter every record
        // in the payload over one wrong-typed field.
        Objects.toString(resourceEntry.get("schemaUrl"), null),
        Objects.toString(scopeEntry.get("schemaUrl"), null));
  }

  /** Appends the envelope to a finished leaf record, omitting whatever the payload lacked. */
  void applyTo(Map<String, Object> record) {
    putIfNotEmpty(record, "resource", resource);
    putIfNotEmpty(record, "scope", scope);
    put(record, "resourceSchemaUrl", resourceSchemaUrl);
    put(record, "scopeSchemaUrl", scopeSchemaUrl);
  }

  /** Null when the scope entry carried no usable scope block. */
  private static Map<String, Object> scope(Map<String, Object> rawScope) {
    if (rawScope == null || rawScope.isEmpty()) {
      return null;
    }
    Map<String, Object> scope = new LinkedHashMap<>();
    put(scope, "name", rawScope.get("name"));
    put(scope, "version", rawScope.get("version"));
    putIfNotEmpty(scope, "attributes", attributes(rawScope.get("attributes")));
    return scope.isEmpty() ? null : scope;
  }
}
