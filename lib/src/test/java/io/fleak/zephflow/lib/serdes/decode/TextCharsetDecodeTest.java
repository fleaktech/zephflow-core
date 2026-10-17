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

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class TextCharsetDecodeTest {

  private static final Charset UTF_32BE = Charset.forName("UTF-32BE");
  private static final Charset UTF_32LE = Charset.forName("UTF-32LE");
  private static final byte[] UTF_16LE_BOM = {(byte) 0xFF, (byte) 0xFE};
  private static final byte[] UTF_16BE_BOM = {(byte) 0xFE, (byte) 0xFF};
  private static final byte[] UTF_32LE_BOM = {(byte) 0xFF, (byte) 0xFE, 0, 0};
  private static final byte[] UTF_32BE_BOM = {0, 0, (byte) 0xFE, (byte) 0xFF};

  private static String text(String charset, String framing) {
    return "{\"format\":{\"type\":\"text\",\"charset\":\""
        + charset
        + "\",\"framing\":{\"type\":"
        + framing
        + "}}}";
  }

  private static String line(String charset) {
    return text(charset, "\"line\"");
  }

  private static String whole(String charset) {
    return text(charset, "\"whole\"");
  }

  private static final List<Object> A_B =
      List.of(new Ok(raw("a"), 1, new RawSpan(0, 1)), new Ok(raw("b"), 2, new RawSpan(2, 1)));

  @Test
  void utf16AndUtf32AreFramedAsUtf8WithoutTheirByteOrderMark() {
    assertEquals(
        A_B,
        decode(line("UTF-16LE"), concat(UTF_16LE_BOM, "a\nb".getBytes(StandardCharsets.UTF_16LE))));
    assertEquals(A_B, decode(line("UTF-16BE"), "a\nb".getBytes(StandardCharsets.UTF_16BE)));
    assertEquals(A_B, decode(line("UTF-32LE"), "a\nb".getBytes(UTF_32LE)));
    assertEquals(A_B, decode(line("UTF-32BE"), concat(UTF_32BE_BOM, "a\nb".getBytes(UTF_32BE))));
  }

  @Test
  void unmarkedUtf16AndUtf32TakeTheirByteOrderFromTheMarkAndDefaultToBigEndian() {
    assertEquals(
        A_B,
        decode(line("UTF-16"), concat(UTF_16LE_BOM, "a\nb".getBytes(StandardCharsets.UTF_16LE))));
    assertEquals(
        A_B,
        decode(line("UTF-16"), concat(UTF_16BE_BOM, "a\nb".getBytes(StandardCharsets.UTF_16BE))));
    assertEquals(A_B, decode(line("UTF-16"), "a\nb".getBytes(StandardCharsets.UTF_16BE)));
    assertEquals(A_B, decode(line("UTF-32"), concat(UTF_32LE_BOM, "a\nb".getBytes(UTF_32LE))));
    assertEquals(A_B, decode(line("UTF-32"), "a\nb".getBytes(UTF_32BE)));
  }

  @Test
  void singleByteCharsetIsTranscodedAndUnmappableBytesAreReplaced() {
    assertEquals(
        List.of(
            new Ok(raw("café"), 1, new RawSpan(0, 5)), new Ok(raw("a�b"), 2, new RawSpan(6, 5))),
        decode(
            line("windows-1252"),
            new byte[] {'c', 'a', 'f', (byte) 0xE9, '\n', 'a', (byte) 0x81, 'b'}));
  }

  @Test
  void onlyTheFirstByteOrderMarkIsDropped() {
    String text = "﻿a";
    List<Object> expected = List.of(new Ok(raw(text), 1, new RawSpan(0, 4)));
    List<byte[]> inputs =
        List.of(
            concat(UTF_32BE_BOM, text.getBytes(UTF_32BE)),
            concat(UTF_32LE_BOM, text.getBytes(UTF_32LE)),
            concat(UTF_16BE_BOM, text.getBytes(StandardCharsets.UTF_16BE)),
            concat(UTF_16LE_BOM, text.getBytes(StandardCharsets.UTF_16LE)));
    List<String> charsets = List.of("UTF-32", "UTF-32LE", "UTF-16BE", "UTF-16");
    for (int i = 0; i < inputs.size(); i++) {
      assertEquals(expected, decode(line(charsets.get(i)), inputs.get(i)), charsets.get(i));
      assertEquals(expected, decode(whole(charsets.get(i)), inputs.get(i)), charsets.get(i));
    }
  }

  @Test
  void utf32SurrogateCodePointsAreEachReplaced() {
    byte[] input = {0, 0, (byte) 0xD8, 0, 0, 0, (byte) 0xDC, 0, 0, 0x11, 0, 0, 0, 0, 0, 'a'};
    assertEquals(
        List.of(new Ok(raw("���a"), 1, new RawSpan(0, 10))), decode(line("UTF-32BE"), input));
  }

  @Test
  void limitsCountTheUtf8Bytes() {
    assertEquals(
        List.of(new Big(1, new RawSpan(0, 4), 4, "éé", false)),
        decode(
            decoder(line("windows-1252")),
            ByteInput.ofBytes(new byte[] {(byte) 0xE9, (byte) 0xE9}),
            new DecodeLimits(3, 0)));
  }

  @Test
  void multilineWorksOnTranscodedText() {
    assertEquals(
        List.of(new Ok(raw("a\r\n b"), 1, new RawSpan(0, 5))),
        decode(
            text("UTF-16LE", "\"multiline\",\"continuePattern\":\"^\\\\s\""),
            "a\r\n b".getBytes(StandardCharsets.UTF_16LE)));
  }

  @Test
  void linesTranscodedBeforeAReadFailureAreKept() {
    byte[] input = "a\nb\nc".getBytes(StandardCharsets.UTF_16LE);
    assertEquals(
        List.of(
            new Ok(raw("a"), 1, new RawSpan(0, 1)),
            new Ok(raw("b"), 2, new RawSpan(2, 1)),
            new Threw("SourceReadException")),
        decode(decoder(line("UTF-16LE")), failingAt(input, 8), DecodeLimits.UNLIMITED));
  }
}
