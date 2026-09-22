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
package io.fleak.zephflow.lib.golden;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Stable serialization, record-by-record diff, and the (deliberately failing) update mode. */
final class GoldenSupport {

  static final boolean UPDATE = Boolean.getBoolean("golden.update");

  /** Keys sorted at every nesting level; one line per record; no pretty-print. */
  private static final ObjectMapper STABLE =
      new ObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  static String stableLine(JsonNode node) throws IOException {
    Object asMap = STABLE.convertValue(node, Map.class);
    return STABLE.writeValueAsString(asMap);
  }

  /** Round-trips a record through the stable form so actual and expected compare identically. */
  static JsonNode readJsonl0(String stableLine) throws IOException {
    return STABLE.readTree(stableLine);
  }

  static List<JsonNode> readJsonl(Path p) throws IOException {
    List<JsonNode> out = new ArrayList<>();
    if (!Files.exists(p)) return out;
    for (String line : Files.readAllLines(p)) {
      if (!line.isBlank()) out.add(STABLE.readTree(line));
    }
    return out;
  }

  static void writeJsonl(Path p, List<ObjectNode> records) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (ObjectNode r : records) sb.append(stableLine(r)).append('\n');
    Files.writeString(p, sb.toString());
  }

  /** Returns human/agent-readable differences, empty if equal. */
  static List<String> diff(String label, List<JsonNode> expected, List<JsonNode> actual) {
    List<String> out = new ArrayList<>();
    if (expected.size() != actual.size()) {
      out.add(label + ": expected " + expected.size() + " records, got " + actual.size());
    }
    int n = Math.min(expected.size(), actual.size());
    for (int i = 0; i < n; i++) {
      List<String> recordDiffs = new ArrayList<>();
      diffNode(label + "[" + i + "]", expected.get(i), actual.get(i), recordDiffs);
      out.addAll(recordDiffs);
      if (out.size() > 40) {
        out.add("... (truncated)");
        break;
      }
    }
    return out;
  }

  private static void diffNode(String path, JsonNode e, JsonNode a, List<String> out) {
    if (e.equals(a)) return;
    if (e.isObject() && a.isObject()) {
      TreeSet<String> keys = new TreeSet<>();
      e.fieldNames().forEachRemaining(keys::add);
      a.fieldNames().forEachRemaining(keys::add);
      for (String k : keys) {
        JsonNode ev = e.get(k), av = a.get(k);
        if (ev == null) out.add(path + "." + k + ": unexpected field, actual=" + av);
        else if (av == null) out.add(path + "." + k + ": missing field, expected=" + ev);
        else diffNode(path + "." + k, ev, av, out);
      }
    } else if (e.isArray() && a.isArray() && e.size() == a.size()) {
      Iterator<JsonNode> ei = e.elements(), ai = a.elements();
      for (int i = 0; ei.hasNext(); i++) diffNode(path + "[" + i + "]", ei.next(), ai.next(), out);
    } else {
      out.add(path + ": expected=" + e + " actual=" + a);
    }
  }
}
