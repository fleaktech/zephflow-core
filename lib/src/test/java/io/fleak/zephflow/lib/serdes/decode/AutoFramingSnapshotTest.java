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
import io.fleak.zephflow.lib.serdes.decode.internal.BudgetCaptureStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

  /** Ends a run that does not end by itself (malformed UTF-16 repeats its input today). */
  private static final int MAXIMUM_OUTCOMES = 100;

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

  /**
   * One input. {@code failAfter} is the byte count after which the source throws (-1: it never
   * does). A {@code wellFormed} input also runs its gzip, and reads in small chunks must give the
   * same outcomes; for a failing or damaged one the small-chunk run is recorded as its own result,
   * since it may fail at another point. {@code inlineRecords} keeps records up to {@link
   * #INLINE_RECORD_CHARACTERS} as JSON, otherwise every record is a digest.
   */
  record Case(
      String name,
      byte[] content,
      long failAfter,
      Map<String, DecodeLimits> profiles,
      boolean wellFormed,
      boolean inlineRecords) {
    @Override
    public String toString() {
      return name;
    }
  }

  static Stream<Arguments> cases() {
    String small =
        "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n{\"id\":3,\"name\":\"c\"}\n";
    byte[] longLineThenLines =
        concat(
            variedLines(0, 30, 500),
            (padded(1, 20_000) + "\n").getBytes(StandardCharsets.UTF_8),
            variedLines(30, 50, 500));
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
        text("empty", ""),
        text(
            "jsonl_line_of_8000_bytes",
            "{\"id\":0}\n" + padded(1, 8000) + "\n" + padded(2, 8000) + "\n{\"id\":3}\n"),
        text(
            "jsonl_line_of_8192_bytes",
            "{\"id\":0}\n" + padded(1, 8192) + "\n" + padded(2, 8192) + "\n{\"id\":3}\n"),
        text(
            "jsonl_multibyte_character_across_fill",
            "{\"id\":0}\n"
                + characterAt(1, 7999, "日")
                + "\n"
                + characterAt(2, 7999, "ż")
                + "\n"
                + characterAt(3, 7998, "日")
                + "\n{\"id\":4}\n"),
        budgetOnFillEnd("json_line_66000_budget_on_fill_end", 66_000, trailingLines),
        budgetOnFillEnd("json_line_71999_budget_on_fill_end", 71_999, trailingLines),
        budgetOnFillEnd("json_line_75000_budget_on_fill_end", 75_000, trailingLines),
        failing("source_fails_during_recognition", variedLines(0, 60, 500), 50),
        failing("source_fails_after_the_first_fill", variedLines(0, 60, 500), 12_345),
        failing("source_fails_at_a_line_end", variedLines(0, 60, 500), 15_000),
        failing("source_fails_inside_a_line", variedLines(0, 60, 500), 15_050),
        failing("source_fails_on_a_fill_boundary", variedLines(0, 60, 500), 15_000 + 8_000),
        failing("source_fails_in_a_long_line_second_fill", longLineThenLines, 15_000 + 9_000),
        failing("source_fails_after_a_long_line", longLineThenLines, 15_000 + 20_001 + 4_050),
        precompressed(
            "gzip_truncated_jsonl",
            Arrays.copyOf(
                gzipOf(variedLines(0, 60, 500)), gzipOf(variedLines(0, 60, 500)).length * 3 / 5)),
        precompressed(
            "gzip_corrupt_second_member",
            concat(gzipOf(variedLines(0, 40, 500)), corrupted(gzipOf(variedLines(40, 80, 500))))),
        precompressed(
            "utf16_malformed_surrogate",
            concat(
                ("\uFEFF" + new String(variedLines(0, 24, 500), StandardCharsets.UTF_8))
                    .getBytes(StandardCharsets.UTF_16LE),
                new byte[] {0x00, (byte) 0xD8},
                new String(variedLines(24, 34, 500), StandardCharsets.UTF_8)
                    .getBytes(StandardCharsets.UTF_16LE))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void autoFramingMatchesExpected(Case c) throws IOException {
    Map<String, Object> actual = new LinkedHashMap<>();
    byte[] compressed = gzipOf(c.content());
    for (var profile : c.profiles().entrySet()) {
      Map<String, Object> plain =
          run(
              () -> new SourceStream(c.content(), false, c.failAfter()),
              profile.getValue(),
              c.inlineRecords());
      Map<String, Object> trickled =
          run(
              () -> new SourceStream(c.content(), true, c.failAfter()),
              profile.getValue(),
              c.inlineRecords());
      actual.put(profile.getKey(), plain);
      if (!c.wellFormed()) {
        actual.put("trickle_" + profile.getKey(), trickled);
        continue;
      }
      assertEquals(
          plain,
          trickled,
          "reads in small chunks give other outcomes for profile " + profile.getKey());
      actual.put(
          "gzip_" + profile.getKey(),
          run(() -> new SourceStream(compressed, false, -1), profile.getValue(), true));
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

  private static Map<String, Object> run(
      ByteInput input, DecodeLimits limits, boolean inlineRecords) {
    List<Map<String, Object>> outcomes = new ArrayList<>();
    String failure = null;
    long normalizedEnd;
    try (RecordCursor cursor = DECODER.open(input, limits)) {
      try {
        DecodeOutcome outcome;
        while ((outcome = cursor.next()) != null) {
          outcomes.add(describe(outcome, inlineRecords));
          if (outcomes.size() == MAXIMUM_OUTCOMES) break;
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
    if (outcomes.size() == MAXIMUM_OUTCOMES) {
      result.put("stoppedAfterMaximumOutcomes", true);
    }
    return result;
  }

  private static Map<String, Object> describe(DecodeOutcome outcome, boolean inlineRecords) {
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
        if (inlineRecords && record.length() <= INLINE_RECORD_CHARACTERS) {
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
    return Arguments.of(new Case(name, content, -1, PROFILES, true, true));
  }

  private static Arguments failing(String name, byte[] content, long failAfter) {
    return Arguments.of(new Case(name, content, failAfter, PROFILES, false, false));
  }

  private static Arguments precompressed(String name, byte[] content) {
    return Arguments.of(new Case(name, content, -1, PROFILES, false, false));
  }

  /**
   * A JSON line of {@code length} after a small one, then {@code trailingLines}, under a document
   * limit whose budget (limit plus read-ahead slack) is exactly nine 8000-byte parser fills.
   */
  private static Arguments budgetOnFillEnd(String name, int length, String trailingLines) {
    long documentLimit = 9 * 8000 - BudgetCaptureStream.READ_AHEAD_SLACK;
    byte[] content =
        ("{\"id\":0}\n" + padded(1, length) + "\n" + trailingLines)
            .getBytes(StandardCharsets.UTF_8);
    return Arguments.of(
        new Case(
            name,
            content,
            -1,
            Map.of(
                "unlimited",
                DecodeLimits.UNLIMITED,
                "fill_aligned",
                new DecodeLimits(1024, documentLimit)),
            true,
            true));
  }

  /**
   * Lines {@code from} to {@code to} (exclusive), each exactly {@code length} bytes with its
   * newline, holding hex digits that compress poorly.
   */
  private static byte[] variedLines(int from, int to, int length) {
    Random random = new Random(from);
    StringBuilder text = new StringBuilder();
    for (int index = from; index < to; index++) {
      String head = String.format("{\"id\":%05d,\"v\":\"", index);
      text.append(head);
      for (int digit = 0; digit < length - head.length() - 3; digit++) {
        text.append(Character.forDigit(random.nextInt(16), 16));
      }
      text.append("\"}\n");
    }
    return text.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** A JSON line whose {@code character} starts at byte {@code offset} of the line. */
  private static String characterAt(int id, int offset, String character) {
    String head = "{\"id\":" + id + ",\"pad\":\"";
    return head + "x".repeat(offset - head.length()) + character + "y".repeat(100) + "\"}";
  }

  /** Flips eight bytes in the middle of a gzip member's compressed data. */
  private static byte[] corrupted(byte[] member) {
    byte[] result = member.clone();
    for (int index = result.length / 2; index < result.length / 2 + 8; index++) {
      result[index] ^= (byte) 0xFF;
    }
    return result;
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

  private static byte[] gzipOf(byte[] content) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
      gzip.write(content);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
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

  /**
   * The content in full reads, or in reads of at most seven bytes so no layer can rely on full
   * reads from its source; it throws on every read once {@code failAfter} bytes are delivered.
   */
  private static final class SourceStream extends InputStream {
    private final byte[] content;
    private final boolean trickle;
    private final long failAfter;
    private int position;

    private SourceStream(byte[] content, boolean trickle, long failAfter) {
      this.content = content;
      this.trickle = trickle;
      this.failAfter = failAfter;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
    }

    @Override
    public int read(byte[] target, int offset, int count) throws IOException {
      if (count == 0) return 0;
      if (position == failAfter) throw new IOException("the source failed after " + failAfter);
      if (position == content.length) return -1;
      long end = failAfter < 0 ? content.length : Math.min(content.length, failAfter);
      int copied =
          (int) Math.min(Math.min(count, trickle ? 1 + position % 7 : count), end - position);
      System.arraycopy(content, position, target, offset, copied);
      position += copied;
      return copied;
    }
  }
}
