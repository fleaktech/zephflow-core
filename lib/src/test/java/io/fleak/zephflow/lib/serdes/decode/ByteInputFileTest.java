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
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ByteInputFileTest {

  @TempDir Path dir;

  @Test
  void streamsTheWholeFile() throws IOException {
    Path file = write("hello\nworld\n");
    try (InputStream in = ByteInput.ofFile(file).openStream()) {
      assertEquals("hello\nworld\n", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void readsBoundedRanges() throws IOException {
    Path file = write("0123456789");
    RandomAccessInput ranges = ByteInput.ofFile(file).randomAccess().orElseThrow();
    assertEquals(10, ranges.length());
    try (InputStream in = ranges.read(3, 4)) {
      assertEquals("3456", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
    try (InputStream in = ranges.read(8, 2)) {
      assertEquals("89", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void zeroLengthRangeIsEmpty() throws IOException {
    Path file = write("0123456789");
    try (InputStream in = ByteInput.ofFile(file).randomAccess().orElseThrow().read(10, 0)) {
      assertEquals(0, in.readAllBytes().length);
    }
  }

  @Test
  void rangePastTheEndEndsEarly() throws IOException {
    Path file = write("0123456789");
    try (InputStream in = ByteInput.ofFile(file).randomAccess().orElseThrow().read(8, 5)) {
      assertEquals("89", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void missingFileFailsOnOpen() {
    ByteInput input = ByteInput.ofFile(dir.resolve("absent"));
    assertThrows(IOException.class, input::openStream);
    assertThrows(IOException.class, () -> input.randomAccess().orElseThrow().read(0, 1));
  }

  @Test
  void decodesThroughTheDecoder() throws IOException {
    Path file = write("{\"a\":1}\n{\"a\":2}\n");
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)),
            new Ok(json("{\"a\":2}"), 2, new RawSpan(8, 7))),
        decode(
            decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"),
            ByteInput.ofFile(file),
            DecodeLimits.UNLIMITED));
  }

  @Test
  void parquetReadsItsFooterAndRowGroupsFromTheFile() throws IOException {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message m { required int64 n; required binary s (STRING); }");
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    List<Group> written = new ArrayList<>();
    List<Object> expected = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      written.add(rows.newGroup().append("n", (long) i).append("s", "v" + i));
      expected.add(new Ok(json("{\"n\":%d,\"s\":\"v%d\"}".formatted(i, i)), i + 1, null));
    }
    Path file = dir.resolve("input.parquet");
    Files.write(file, ParquetTestFiles.write(schema, written, options -> {}));
    assertEquals(
        expected,
        decode(
            decoder("{\"format\":{\"type\":\"parquet\"}}"),
            ByteInput.ofFile(file),
            DecodeLimits.UNLIMITED));
  }

  private Path write(String content) throws IOException {
    Path file = dir.resolve("input");
    Files.writeString(file, content);
    return file;
  }
}
