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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class AvroDecodeTest {

  private static final String OCF = "{\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}";

  private static final Schema ALL_TYPES =
      new Schema.Parser()
          .parse(
              """
              {"type":"record","name":"All","fields":[
                {"name":"s","type":"string"},
                {"name":"i","type":"int"},
                {"name":"l","type":"long"},
                {"name":"d","type":"double"},
                {"name":"b","type":"boolean"},
                {"name":"n","type":["null","string"]},
                {"name":"u","type":["null","long"]},
                {"name":"arr","type":{"type":"array","items":"int"}},
                {"name":"m","type":{"type":"map","values":"string"}},
                {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
                {"name":"by","type":"bytes"},
                {"name":"fx","type":{"type":"fixed","name":"F","size":2}},
                {"name":"tsMicros","type":{"type":"long","logicalType":"timestamp-micros"}},
                {"name":"tsMillis","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"local","type":{"type":"long","logicalType":"local-timestamp-micros"}},
                {"name":"date","type":{"type":"int","logicalType":"date"}},
                {"name":"timeMillis","type":{"type":"int","logicalType":"time-millis"}},
                {"name":"timeMicros","type":{"type":"long","logicalType":"time-micros"}},
                {"name":"dec","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
                {"name":"inf","type":"double"},
                {"name":"nested","type":{"type":"record","name":"N","fields":[{"name":"x","type":"int"}]}}
              ]}
              """);

  private static final Schema SMALL =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"v\",\"type\":\"string\"}]}");

  private static GenericRecord allTypes(int i) {
    GenericRecord nested = new GenericData.Record(ALL_TYPES.getField("nested").schema());
    nested.put("x", i);
    GenericRecord r = new GenericData.Record(ALL_TYPES);
    r.put("s", "s" + i);
    r.put("i", i);
    r.put("l", 1L << 40);
    r.put("d", 1.5);
    r.put("b", true);
    r.put("n", null);
    r.put("u", 5L);
    r.put("arr", List.of(1, 2));
    r.put("m", Map.of("k", "v"));
    r.put("e", new GenericData.EnumSymbol(ALL_TYPES.getField("e").schema(), "B"));
    r.put("by", ByteBuffer.wrap(new byte[] {1, 2, 3}));
    r.put("fx", new GenericData.Fixed(ALL_TYPES.getField("fx").schema(), new byte[] {9, 8}));
    r.put("tsMicros", 1_705_312_800_123_456L);
    r.put("tsMillis", 1_705_312_800_123L);
    r.put("local", -1L);
    r.put("date", 19_737);
    r.put("timeMillis", 3_600_000);
    r.put("timeMicros", 3_600_000_999L);
    Schema decimal = ALL_TYPES.getField("dec").schema();
    r.put(
        "dec",
        new Conversions.DecimalConversion()
            .toBytes(new BigDecimal("12345.67"), decimal, LogicalTypes.decimal(10, 2)));
    r.put("inf", Double.POSITIVE_INFINITY);
    r.put("nested", nested);
    return r;
  }

  private static Ok allTypesRecord(int i, long index) {
    return new Ok(
        json(
            "{\"s\":\"s"
                + i
                + "\",\"i\":"
                + i
                + ",\"l\":1099511627776,\"d\":1.5,\"b\":true,\"u\":5,\"arr\":[1,2],\"m\":{\"k\":\"v\"},"
                + "\"e\":\"B\",\"by\":\"AQID\",\"fx\":\"CQg=\",\"tsMicros\":1705312800123,"
                + "\"tsMillis\":1705312800123,\"local\":-1,\"date\":19737,\"timeMillis\":3600000,"
                + "\"timeMicros\":3600000,\"dec\":\"12345.67\",\"inf\":\"Infinity\",\"nested\":{\"x\":"
                + i
                + "}}"),
        index,
        null);
  }

  static Stream<String> codecs() {
    return Stream.of("null", "deflate", "snappy", "zstandard", "bzip2", "xz");
  }

  private static CodecFactory codec(String name) {
    return CodecFactory.fromString(name);
  }

  /** Writes each list of records as one block. */
  @SafeVarargs
  private static byte[] ocf(Schema schema, String codec, List<GenericRecord>... blocks) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.setCodec(codec(codec));
      writer.create(schema, bytes);
      for (List<GenericRecord> block : blocks) {
        for (GenericRecord record : block) {
          writer.append(record);
        }
        writer.sync();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  private static GenericRecord small(String value) {
    GenericRecord record = new GenericData.Record(SMALL);
    record.put("v", value);
    return record;
  }

  private static Ok smallRecord(String value, long index) {
    return new Ok(json("{\"v\":\"" + value + "\"}"), index, null);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void everyCodecAndTypeDecodes(String codec) {
    assertEquals(
        List.of(allTypesRecord(0, 1), allTypesRecord(1, 2), allTypesRecord(2, 3)),
        decode(
            OCF, ocf(ALL_TYPES, codec, List.of(allTypes(0), allTypes(1)), List.of(allTypes(2)))));
  }

  @Test
  void emptyAndNonAvroInput() {
    assertEquals(List.of(), decode(OCF, new byte[0]));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                null,
                "MalformedAvroException: input is not an avro object container file",
                true)),
        decode(OCF, utf8("not avro")));
  }

  @Test
  void blockOverTheDocumentLimitIsSkippedAndTheNextBlockDecodes() {
    byte[] file =
        ocf(
            SMALL,
            "null",
            List.of(small("a")),
            List.of(small("x".repeat(5_000)), small("y")),
            List.of(small("b")));
    List<Object> actual = decode(decoder(OCF), ByteInput.ofBytes(file), new DecodeLimits(0, 1_000));
    assertEquals(
        List.of(
            smallRecord("a", 1),
            new Big(2, null, ((Big) actual.get(1)).sizeSoFar(), "", false),
            smallRecord("b", 4)),
        actual);
  }

  @Test
  void blockThatDecompressesOverTheLimitIsSkipped() {
    byte[] file =
        ocf(
            SMALL,
            "deflate",
            List.of(small("a")),
            List.of(small("x".repeat(100_000))),
            List.of(small("b")));
    assertEquals(
        List.of(smallRecord("a", 1), new Big(2, null, 8_192, "", false), smallRecord("b", 3)),
        decode(decoder(OCF), ByteInput.ofBytes(file), new DecodeLimits(0, 5_000)));
  }

  @Test
  void recordThatDoesNotDecodeLosesOnlyItsBlock() {
    byte[] file =
        ocf(
            SMALL,
            "null",
            List.of(small("a")),
            List.of(small("bb"), small("cc")),
            List.of(small("d")));
    byte[] damaged = file.clone();
    int at = indexOf(damaged, utf8("bb")) - 1;
    damaged[at] = (byte) 0x7E;
    assertEquals(
        List.of(
            smallRecord("a", 1),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "MalformedAvroException: string of 63 bytes is longer than the 5 bytes left",
                false),
            smallRecord("d", 4)),
        decode(OCF, damaged));
  }

  @Test
  void corruptCompressedBlockLosesOnlyItsBlock() {
    byte[] file =
        ocf(
            SMALL,
            "deflate",
            List.of(small("a")),
            List.of(small("b".repeat(1_000))),
            List.of(small("c")));
    byte[] second = ocf(SMALL, "deflate", List.of(small("a")));
    byte[] damaged = file.clone();
    // Block 2 starts right after block 1's sync: a 1-byte count, a 1-byte size, then deflate data,
    // whose first byte 0xFF declares the invalid block type 3.
    damaged[second.length + 2] = (byte) 0xFF;
    assertEquals(
        List.of(
            smallRecord("a", 1),
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                2,
                null,
                "CorruptBlockException: avro deflate block is corrupt: invalid block type",
                false),
            smallRecord("c", 3)),
        decode(OCF, damaged));
  }

  @Test
  void truncationAndAWrongSyncMarkerAreTerminal() {
    byte[] file = ocf(SMALL, "null", List.of(small("a")), List.of(small("b")));
    byte[] oneBlock = ocf(SMALL, "null", List.of(small("a")));
    assertEquals(List.of(smallRecord("a", 1)), decode(OCF, Arrays.copyOf(file, oneBlock.length)));
    assertEquals(
        List.of(
            smallRecord("a", 1),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "MalformedAvroException: avro file is truncated",
                true)),
        decode(OCF, Arrays.copyOf(file, file.length - 5)));
    byte[] badSync = file.clone();
    badSync[badSync.length - 1] ^= 1;
    assertEquals(
        List.of(
            smallRecord("a", 1),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "MalformedAvroException: avro block 2 is not followed by the sync marker",
                true)),
        decode(OCF, badSync));
  }

  @Test
  void readFailureIsNotMalformed() {
    byte[] file = ocf(SMALL, "null", List.of(small("a")), List.of(small("b")));
    assertEquals(
        List.of(new Threw("SourceReadException")),
        decode(decoder(OCF), failingAt(file, 20), DecodeLimits.UNLIMITED));
  }

  private static String datumConfig(Schema schema) {
    return "{\"format\":{\"type\":\"avro\",\"container\":\"datum\",\"schema\":" + schema + "}}";
  }

  private static byte[] datum(GenericRecord record) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
    try {
      new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  @Test
  void datumIsTheWholeInput() {
    byte[] input = datum(allTypes(7));
    assertEquals(
        List.of(new Ok(allTypesRecord(7, 1).record(), 1, new RawSpan(0, input.length))),
        decode(datumConfig(ALL_TYPES), input));
    byte[] trailing = concat(input, new byte[] {0});
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(trailing, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, trailing.length),
                "MalformedAvroException: 1 bytes are left after the avro datum",
                true)),
        decode(datumConfig(ALL_TYPES), trailing));
  }

  @Test
  void emptyDatumDecodesWhenTheSchemaFitsInZeroBytes() {
    Schema nulls =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Z\",\"fields\":[{\"name\":\"z\",\"type\":\"null\"}]}");
    assertEquals(
        List.of(new Ok(json("{}"), 1, new RawSpan(0, 0))), decode(datumConfig(nulls), new byte[0]));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                -1,
                new RawSpan(0, 0),
                "MalformedAvroException: varint of 1 bytes is longer than the 0 bytes left",
                true)),
        decode(datumConfig(SMALL), new byte[0]));
  }

  private static int indexOf(byte[] haystack, byte[] needle) {
    outer:
    for (int i = 0; i <= haystack.length - needle.length; i++) {
      for (int j = 0; j < needle.length; j++) {
        if (haystack[i + j] != needle[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }
}
