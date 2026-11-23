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
package io.fleak.zephflow.lib.serdes.decode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fleak.zephflow.lib.serdes.decode.auto.AutoFormatCursor;
import io.fleak.zephflow.lib.serdes.decode.config.AutoFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every outcome automatic format recognition gives for a fixed set of files (frame boundaries,
 * frame bytes, decoded records, failures), against expected results under {@code
 * serdes/autoframing/}. A change of the framer must change only the expected results of the cases
 * it means to change. {@code -Dautoframing.snapshot.update=true} rewrites them from the current
 * code and fails, so that the diff is reviewed.
 */
class AutoFramingSnapshotTest {

  private static final Path EXPECTED = Path.of("src/test/resources/serdes/autoframing");
  private static final boolean UPDATE = Boolean.getBoolean("autoframing.snapshot.update");
  private static final int INLINE_RECORD_CHARACTERS = 2048;
  private static final Map<String, DecodeLimits> PROFILES =
      Map.of(
          "unlimited", DecodeLimits.UNLIMITED,
          "tight", new DecodeLimits(1024, 4096),
          "medium", new DecodeLimits(100_000, 150_000));
  private static final RecordDecoder DECODER =
      DecodingConfigCompiler.compile(
          new DecodingConfig(List.of(DecompressionCodec.AUTO), new AutoFormatConfig()),
          AutoFormatCursor.Unit.FILE);
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(SerializationFeature.INDENT_OUTPUT)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  record Case(String name, byte[] content) {
    @Override
    public String toString() {
      return name;
    }
  }

  static Stream<Arguments> cases() {
    String small =
        "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n{\"id\":3,\"name\":\"c\"}\n";
    String trailingLines =
        IntStream.range(100, 108)
            .mapToObj(i -> padded(i, 1000) + "\n")
            .collect(Collectors.joining());
    return Stream.of(
        text("jsonl_small", small),
        text(
            "jsonl_large_lines",
            "{\"id\":1}\n"
                + padded(2, 9_000)
                + "\n"
                + padded(3, 70_000)
                + "\n"
                + padded(4, 200_000)
                + "\n{\"id\":5}\n"),
        text("jsonl_last_line_without_newline", "{\"id\":1}\n{\"id\":2}\n{\"id\":3}"),
        text("jsonl_crlf", "{\"id\":1}\r\n{\"id\":2}\r\n\r\n{\"id\":3}\r\n"),
        text("jsonl_blank_lines", "\n{\"id\":1}\n\n   \n\t\n{\"id\":2}\n\n\n"),
        text("jsonl_bom", "﻿{\"id\":1}\n{\"id\":2}\n"),
        bytes("jsonl_utf16le_bom", "﻿{\"id\":1}\n{\"id\":2}\n".getBytes(StandardCharsets.UTF_16LE)),
        text(
            "jsonl_nested",
            "{\"a\":{\"b\":[1,{\"c\":[2,3]},[4,[5]]]},\"d\":[]}\n[{\"e\":{}},[[]]]\n{\"f\":{\"g\":{\"h\":null}}}\n"),
        text(
            "jsonl_strings_with_structure_characters",
            "{\"a\":\"{\",\"b\":\"}\"}\n{\"q\":\"say \\\"hi\\\" {now}\"}\n{\"n\":\"line\\nbreak\\r\\t\"}\n{\"s\":\"[\\\\\\\"]\"}\n"),
        text("jsonl_non_ascii", "{\"name\":\"żółć\"}\n{\"emoji\":\"😀\"}\n{\"jp\":\"日本語\"}\n"),
        bytes(
            "jsonl_invalid_utf8",
            concat(
                "{\"id\":1}\n{\"bad\":\"".getBytes(StandardCharsets.UTF_8),
                new byte[] {(byte) 0xC3, (byte) 0x28},
                "\"}\n{\"id\":3}\n".getBytes(StandardCharsets.UTF_8))),
        text("jsonl_truncated_at_end", "{\"id\":1}\n{\"id\":2}\n{\"id\":3,\"name\":\"tr"),
        text("json_top_level_array", "[\n  {\"id\":1},\n  {\"id\":2}\n]\n"),
        text("json_array_lines", "[1,2,3]\n[{\"a\":1}]\n[]\n"),
        text(
            "json_concatenated_without_newline",
            "{\"id\":1}{\"id\":2}[3]{\"id\":4}\n{\"id\":5} {\"id\":6}\n"),
        text("json_followed_by_text_on_line", "{\"id\":1} trailing text\n{\"id\":2}\n"),
        text("json_leading_whitespace", "  {\"id\":1}\n\t{\"id\":2}  \n \t[3]\t\n"),
        text("json_multiline_documents", "{\n  \"id\": 1\n}\n{\n  \"id\": 2\n}\n"),
        text(
            "syslog_like",
            "<34>Oct 11 22:14:15 host app[123]: started\n"
                + "<13>1 2003-10-11T22:14:15.003Z host app 123 ID47 - hello\n"
                + "Oct 11 22:14:16 host sshd[42]: Accepted publickey for alice\n"),
        text(
            "syslog_like_with_quoted_colon",
            "<34>Oct 11 22:14:15 host app: {\"user\": \"alice\"}\n"
                + "<34>Oct 11 22:14:16 host app: {\"user\": \"bob\"}\n"
                + "<34>Oct 11 22:14:17 host app: {\"user\": \"carol\"}\n"),
        text(
            "csv_like",
            "id,name,note\n1,alice,\"likes, commas\"\n2,bob,\"says \"\"hi\"\"\"\n3,carol,\"two\nlines\"\n"),
        text("key_value", "a=1 b=2 c=\"x y\"\nuser=alice action=login ok=true\nk=v\n"),
        text(
            "cef",
            "CEF:0|vendor|product|1|100|login|5|src=10.0.0.1 msg=hello world\n"
                + "CEF:0|vendor|product|1|101|logout|3|src=10.0.0.2 suser=bob\n"),
        text("yaml_single_document", "a: 1\nb:\n  - x\n  - y\n"),
        text("yaml_documents", "---\na: 1\n---\nb: 2\n---\n- x\n"),
        text("raw_text", "hello world\nthis is plain text\n\nlast line"),
        text(
            "mixed",
            "{\"id\":1}\n"
                + "<34>Oct 11 22:14:15 host app[123]: started\n"
                + "a,b,c\n"
                + "\n"
                + "plain words here\n"
                + "[1,2]\n"
                + "k=v x=y\n"
                + "CEF:0|vendor|product|1|100|login|5|src=10.0.0.1\n"
                + "{\"id\":2,\"nested\":{\"a\":[1,2]}}\n"
                + "{broken json\n"
                + "{\"id\":3}\n"),
        text(
            "json_line_63000_then_lines",
            "{\"id\":0}\n" + padded(1, 63_000) + "\n" + trailingLines),
        text(
            "json_line_66000_then_lines",
            "{\"id\":0}\n" + padded(1, 66_000) + "\n" + trailingLines),
        text(
            "json_line_72000_then_lines",
            "{\"id\":0}\n" + padded(1, 72_000) + "\n" + trailingLines),
        text(
            "json_line_210000_then_lines",
            "{\"id\":0}\n" + padded(1, 210_000) + "\n" + trailingLines),
        text("empty", ""));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void autoFramingMatchesExpected(Case c) throws IOException {
    Map<String, Object> actual = new LinkedHashMap<>();
    byte[] compressed = gzip(c.content());
    for (var profile : PROFILES.entrySet()) {
      Map<String, Object> plain =
          run(() -> new ByteArrayInputStream(c.content()), profile.getValue());
      assertEquals(
          plain,
          run(() -> new TrickleInputStream(c.content()), profile.getValue()),
          "reads in small chunks give other outcomes for profile " + profile.getKey());
      actual.put(profile.getKey(), plain);
      actual.put(
          "gzip_" + profile.getKey(),
          run(() -> new ByteArrayInputStream(compressed), profile.getValue()));
    }
    JsonNode actualTree = MAPPER.readTree(MAPPER.writeValueAsString(actual));

    Path expectedFile = EXPECTED.resolve(c.name() + ".json");
    if (UPDATE) {
      Files.createDirectories(EXPECTED);
      MAPPER.writeValue(expectedFile.toFile(), actualTree);
      fail("updated " + expectedFile + "; review the diff and run again without the update flag");
    }
    assertEquals(MAPPER.readTree(expectedFile.toFile()), actualTree);
  }

  @Test
  void everyExpectedFileHasACase() throws IOException {
    try (Stream<Path> files = Files.list(EXPECTED)) {
      assertEquals(
          cases().map(a -> ((Case) a.get()[0]).name() + ".json").collect(Collectors.toSet()),
          files
              .map(f -> f.getFileName().toString())
              .filter(n -> n.endsWith(".json"))
              .collect(Collectors.toSet()));
    }
  }

  private static Map<String, Object> run(ByteInput input, DecodeLimits limits) {
    List<Map<String, Object>> outcomes = new ArrayList<>();
    String failure = null;
    long normalizedEnd;
    try (RecordCursor cursor = DECODER.open(input, limits)) {
      try {
        DecodeOutcome outcome;
        while ((outcome = cursor.next()) != null) {
          outcomes.add(describe(outcome));
        }
      } catch (SourceReadException exception) {
        failure = describe(exception);
      }
      normalizedEnd = cursor.normalizedEndOffset();
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("outcomes", outcomes);
    result.put("failure", failure);
    result.put("normalizedEnd", normalizedEnd);
    return result;
  }

  private static Map<String, Object> describe(DecodeOutcome outcome) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("recordIndex", outcome.recordIndex());
    result.put("recordCount", outcome.recordCount());
    result.put("offset", outcome.span() == null ? null : outcome.span().offset());
    result.put("length", outcome.span() == null ? null : outcome.span().length());
    result.put("terminal", outcome.terminal());
    switch (outcome) {
      case DecodeOutcome.Decoded decoded -> {
        result.put("type", "decoded");
        String record = writeJson(decoded.record().unwrap());
        if (record.length() <= INLINE_RECORD_CHARACTERS) {
          result.put("record", decoded.record().unwrap());
        } else {
          result.put("recordJsonLength", record.length());
          result.put("recordJsonSha256", sha256(record.getBytes(StandardCharsets.UTF_8)));
        }
      }
      case DecodeOutcome.Failed failed -> {
        result.put("type", "failed");
        result.put("kind", failed.kind().name());
        result.put("rawLength", failed.raw().length);
        result.put("rawSha256", sha256(failed.raw()));
        result.put("error", describe(failed.error()));
      }
      case DecodeOutcome.Oversized oversized -> {
        result.put("type", "oversized");
        result.put("limit", oversized.limit().name());
        result.put("limitValue", oversized.limitValue());
        result.put("sizeSoFar", oversized.sizeSoFar());
        result.put("previewLength", oversized.preview().length);
        result.put("previewSha256", sha256(oversized.preview()));
      }
    }
    return result;
  }

  private static String describe(Throwable error) {
    StringBuilder text = new StringBuilder();
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (!text.isEmpty()) text.append(" <- ");
      text.append(cause.getClass().getName()).append(": ").append(cause.getMessage());
    }
    return text.toString();
  }

  private static Arguments text(String name, String content) {
    return bytes(name, content.getBytes(StandardCharsets.UTF_8));
  }

  private static Arguments bytes(String name, byte[] content) {
    return Arguments.of(new Case(name, content));
  }

  /** A one-line JSON object of exactly {@code length} bytes. */
  private static String padded(int id, int length) {
    String head = "{\"id\":" + id + ",\"pad\":\"";
    String tail = "\"}";
    return head + "x".repeat(length - head.length() - tail.length()) + tail;
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (byte[] part : parts) output.writeBytes(part);
    return output.toByteArray();
  }

  private static byte[] gzip(byte[] content) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
      gzip.write(content);
    }
    return output.toByteArray();
  }

  private static String writeJson(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (IOException exception) {
      throw new IllegalStateException(exception);
    }
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException(exception);
    }
  }

  /** Returns at most seven bytes per read, so no layer can rely on full reads from its source. */
  private static final class TrickleInputStream extends InputStream {
    private final byte[] content;
    private int position;

    private TrickleInputStream(byte[] content) {
      this.content = content;
    }

    @Override
    public int read() {
      return position == content.length ? -1 : content[position++] & 255;
    }

    @Override
    public int read(byte[] target, int offset, int count) {
      if (count == 0) return 0;
      if (position == content.length) return -1;
      int copied = Math.min(Math.min(count, 1 + position % 7), content.length - position);
      System.arraycopy(content, position, target, offset, copied);
      position += copied;
      return copied;
    }
  }
}
