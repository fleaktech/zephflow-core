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

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KvEncodeTest {

  @TempDir Path dir;

  static Stream<Arguments> lines() {
    return Stream.of(
        line(
            "every field sorted by name when none are pinned",
            kv(""),
            event("src_ip", "10.0.4.17", "port", 443, "allowed", true),
            "allowed=true port=443 src_ip=10.0.4.17"),
        line(
            "pinned fields set the order and skip null or missing",
            kv(",\"fields\":[\"src_ip\",\"dst_ip\",\"user\",\"action\"]"),
            event("action", "allow", "src_ip", "10.0.4.17", "user", null),
            "src_ip=10.0.4.17 action=allow"),
        line(
            "empty fields mean every field",
            kv(",\"fields\":[]"),
            event("a", 1, "b", 2),
            "a=1 b=2"),
        line(
            "empty fields mean every non-null field",
            kv(",\"fields\":[]"),
            event("a", 1, "b", null, "c", 2),
            "a=1 c=2"),
        line(
            "values that would break parsing are quoted and escaped",
            kv(""),
            event(
                "msg", "user logged in",
                "q", "say \"hi\"",
                "path", "C:\\Windows",
                "eq", "a=b",
                "multi", "line1\nline2",
                "empty", ""),
            "empty=\"\" eq=\"a=b\" msg=\"user logged in\" multi=\"line1\\nline2\""
                + " path=\"C:\\\\Windows\" q=\"say \\\"hi\\\"\""),
        line(
            "a tab is quoted but kept and a carriage return is escaped",
            kv(""),
            event("t", "a\tb", "cr", "x\ry"),
            "cr=\"x\\ry\" t=\"a\tb\""),
        line(
            "one value holding a carriage return, a line feed and a tab",
            kv(""),
            event("v", "1\r\n2\t3"),
            "v=\"1\\r\\n2\t3\""),
        line(
            "values outside the safe set are quoted",
            kv(""),
            event(
                "path", "a-b.c:d/e_f9",
                "csv", "a,b",
                "semi", "a;b",
                "nbsp", "a b",
                "at", "user@example.com",
                "plus", "+1"),
            "at=\"user@example.com\" csv=\"a,b\" nbsp=\"a b\" path=a-b.c:d/e_f9 plus=\"+1\""
                + " semi=\"a;b\""),
        line(
            "non-ASCII is written as UTF-8",
            kv(""),
            event("user", "\u5f20\u4f1f", "city", "Z\u00fcrich"),
            "city=\"Z\u00fcrich\" user=\"\u5f20\u4f1f\""),
        line(
            "numbers and booleans keep their JSON-like form",
            kv(",\"fields\":[\"port\",\"neg\",\"ratio\",\"big\",\"ok\"]"),
            event("port", 443, "neg", -3L, "ratio", 1.5, "big", 1.0e10, "ok", false),
            "port=443 neg=-3 ratio=1.5 big=1.0E10 ok=false"),
        line(
            "nested values are written as text",
            kv(""),
            event("device", map("os", "win"), "tags", List.of("a", "b")),
            "device=\"{os=win}\" tags=\"[a, b]\""),
        line(
            "empty and null-holding nested values",
            kv(""),
            event("m", map(), "l", List.of(), "n", map("a", null)),
            "l=\"[]\" m=\"{}\" n=\"{a=null}\""),
        line(
            "a custom pair delimiter quotes values containing it",
            kv(",\"pairDelimiter\":\", \""),
            event("a", "x, y", "b", "plain"),
            "a=\"x, y\", b=plain"),
        line(
            "a tab pair delimiter quotes values containing tabs",
            kv(",\"pairDelimiter\":\"\\t\""),
            event("a", 1, "b", "x\ty"),
            "a=1\tb=\"x\ty\""),
        line(
            "a non-whitespace delimiter quotes values containing it",
            kv(",\"pairDelimiter\":\"|\""),
            event("a", "x|y", "b", "|", "c", "plain"),
            "a=\"x|y\"|b=\"|\"|c=plain"),
        line(
            "keys containing a comma delimiter are sanitized",
            kv(",\"pairDelimiter\":\",\""),
            event("a,b", 1, "c", 2),
            "a_b=1,c=2"),
        line(
            "keys containing a pipe delimiter are sanitized",
            kv(",\"pairDelimiter\":\"|\""),
            event("a|b", 1, "c", 2),
            "a_b=1|c=2"),
        line(
            "keys that would break parsing are sanitized without cleanFields",
            kv(""),
            event("user name", "a", "k=v", "b", "q\"k", "c", "", "d", "event.id", "e"),
            "_=d event.id=e k_v=b q_k=c user_name=a"),
        line(
            "cleanFields replaces non-alphanumeric key characters",
            kv(",\"cleanFields\":true"),
            event("event.id", 4624, "Logon-Type", 3),
            "event_id=4624 Logon_Type=3"),
        line(
            "keys are kept as they are without cleanFields",
            kv(""),
            event("event.id", 4624, "Logon-Type", 3),
            "event.id=4624 Logon-Type=3"),
        line(
            "cleanFields keeps keys that collide after cleaning",
            kv(",\"cleanFields\":true"),
            event("a.b", 1, "a-b", 2),
            "a_b=2 a_b_2=1"),
        line(
            "colliding keys get numbered suffixes that stay unique",
            kv(",\"cleanFields\":true"),
            event("a.b", 1, "a_b", 2, "a_b_2", 3),
            "a_b=1 a_b_2=2 a_b_2_2=3"),
        line(
            "collisions from parse-breaking key characters are suffixed too",
            kv(""),
            event("a b", 1, "a_b", 2),
            "a_b=1 a_b_2=2"),
        line(
            "pinned fields are looked up by original name and cleaned on output",
            kv(",\"fields\":[\"Logon-Type\",\"event.id\"],\"cleanFields\":true"),
            event("event.id", 4624, "Logon-Type", 3, "other", "x"),
            "Logon_Type=3 event_id=4624"),
        line(
            "pinned fields are looked up by their raw name",
            kv(",\"fields\":[\" action\",\"a.b\"]"),
            event(" action", "allow", "a.b", "top", "a", map("b", "nested")),
            "_action=allow a.b=top"),
        line(
            "unpinned keys sort ignoring case and keep case variants",
            kv(""),
            event("Logon_Type", 3, "a", 2, "action", "x", "A", 1),
            "A=1 a=2 action=x Logon_Type=3"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("lines")
  void writesTheLegacyLineInRecordAndFileMode(
      String name, String format, RecordFleakData record, String expected) throws Exception {
    EncodingConfig config = encoding("[]", format);
    assertEquals(
        List.of(expected, expected),
        List.of(
            utf8(EncodingCompiler.recordEncoder(config, KAFKA).encode(record)),
            utf8(writeOneFile(EncodingCompiler.fileWriter(config, S3), List.of(record), dir))));
  }

  @Test
  void fileModeWritesOneLinePerRecordAsRawFiles() throws Exception {
    BlobFileWriter<RecordFleakData> writer =
        EncodingCompiler.fileWriter(encoding("[]", kv("")), S3);
    assertEquals(
        List.of("a=1\na=2 b=x", "", "raw", "raw.gz"),
        List.of(
            utf8(writeOneFile(writer, List.of(event("a", 1), event("a", 2, "b", "x")), dir)),
            utf8(writeOneFile(writer, List.of(), dir)),
            writer.getFileExtension(),
            EncodingCompiler.fileWriter(encoding("[\"gzip\"]", kv("")), S3).getFileExtension()));
  }

  @Test
  void recordsWithNothingToWriteAreRefused() {
    EncodingConfig unpinned = encoding("[]", kv(""));
    EncodingConfig pinned = encoding("[]", kv(",\"fields\":[\"src_ip\"]"));
    RecordEncoder unpinnedRecords = EncodingCompiler.recordEncoder(unpinned, KAFKA);
    BlobFileWriter<RecordFleakData> unpinnedFiles = EncodingCompiler.fileWriter(unpinned, S3);
    BlobFileWriter<RecordFleakData> pinnedFiles = EncodingCompiler.fileWriter(pinned, S3);
    String noValues = "event has no non-null fields to write as KV";
    String noFields = "event has none of the configured KV fields";
    assertEquals(
        List.of(noValues, noValues, noValues, noValues, noFields, noFields, noFields),
        List.of(
            failure(() -> unpinnedRecords.encode(json("{}"))),
            failure(() -> unpinnedRecords.encode(json("{\"a\":null}"))),
            failure(() -> unpinnedFiles.validateRecord(json("{}"))),
            failure(() -> unpinnedFiles.validateRecord(json("{\"a\":null}"))),
            failure(() -> EncodingCompiler.recordEncoder(pinned, KAFKA).encode(event("other", 1))),
            failure(() -> pinnedFiles.validateRecord(event("other", "x"))),
            failure(
                () ->
                    pinnedFiles.writeToTempFiles(
                        List.of(event("src_ip", 1), event("other", 2)), dir))));
    assertDoesNotThrow(() -> pinnedFiles.validateRecord(event("src_ip", "x")));
  }

  @Test
  void nestedValuesMatchTheCsvText() throws Exception {
    Map<String, Object> person = map("n", "John");
    List<Object> shapes =
        List.of(
            map("os", "win", "tags", List.of("a", "b")),
            map("port", 443, "ratio", 1.5, "big", 1.0e10, "ok", true),
            List.of(person, person),
            List.of(1, 2),
            map());
    RecordEncoder kv =
        EncodingCompiler.recordEncoder(encoding("[]", kv(",\"fields\":[\"v\"]")), KAFKA);
    RecordEncoder csv =
        EncodingCompiler.recordEncoder(
            encoding("[]", "{\"type\":\"csv\",\"header\":false,\"columns\":[\"v\"]}"), KAFKA);
    List<String> expected = new ArrayList<>();
    List<String> actual = new ArrayList<>();
    for (Object shape : shapes) {
      RecordFleakData record = event("v", shape);
      expected.add("v=\"" + csvText(csv.encode(record)) + "\"");
      actual.add(utf8(kv.encode(record)));
    }
    assertEquals(expected, actual);
  }

  private static String csvText(byte[] bytes) {
    String line = utf8(bytes).strip();
    return line.startsWith("\"")
        ? line.substring(1, line.length() - 1).replace("\"\"", "\"")
        : line;
  }

  private static Arguments line(
      String name, String format, RecordFleakData record, String expected) {
    return Arguments.of(name, format, record, expected);
  }

  private static String kv(String options) {
    return "{\"type\":\"kv\"" + options + "}";
  }

  private static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  private static RecordFleakData event(Object... keysAndValues) {
    return (RecordFleakData) FleakData.wrap(map(keysAndValues));
  }

  private static String failure(Executable action) {
    return assertThrows(IllegalArgumentException.class, action).getMessage();
  }

  private static String utf8(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
