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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
    long blockSize = datum(small("x".repeat(5_000))).length + datum(small("y")).length;
    assertEquals(
        List.of(smallRecord("a", 1), new Big(2, null, blockSize, "", false), smallRecord("b", 4)),
        decode(decoder(OCF), ByteInput.ofBytes(file), new DecodeLimits(0, 1_000)));
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

  /** An object container file with the given metadata and raw blocks, using a fixed sync. */
  private static byte[] craftedOcf(Map<String, String> metadata, byte[]... blocks) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      bytes.write(new byte[] {'O', 'b', 'j', 1});
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
      encoder.writeLong(metadata.size());
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        encoder.writeString(entry.getKey());
        encoder.writeBytes(utf8(entry.getValue()));
      }
      encoder.writeLong(0);
      encoder.flush();
      bytes.write(SYNC);
      for (byte[] block : blocks) {
        bytes.write(block);
      }
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final byte[] SYNC = new byte[16];

  private static byte[] rawBlock(long count, byte[] data) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
      encoder.writeLong(count);
      encoder.writeLong(data.length);
      encoder.flush();
      bytes.write(data);
      bytes.write(SYNC);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<String, String> schemaMeta(Schema schema) {
    return Map.of("avro.schema", schema.toString());
  }

  private static Bad malformed(long index, String message, boolean terminal) {
    return new Bad(
        ErrorKind.MALFORMED, "", index, null, "MalformedAvroException: " + message, terminal);
  }

  @Test
  void remainingTypeMappingRows() {
    Schema schema =
        new Schema.Parser()
            .parse(
                """
                {"type":"record","name":"T","fields":[
                  {"name":"lms","type":{"type":"long","logicalType":"local-timestamp-millis"}},
                  {"name":"ns","type":{"type":"long","logicalType":"timestamp-nanos"}},
                  {"name":"lns","type":{"type":"long","logicalType":"local-timestamp-nanos"}},
                  {"name":"fdec","type":{"type":"fixed","name":"D","size":4,"logicalType":"decimal","precision":8,"scale":3}},
                  {"name":"us","type":{"type":"string","logicalType":"uuid"}},
                  {"name":"uf","type":{"type":"fixed","name":"U","size":16,"logicalType":"uuid"}},
                  {"name":"multi","type":["null","int","string"]},
                  {"name":"f","type":"float"},
                  {"name":"nan","type":"double"},
                  {"name":"ninf","type":"double"},
                  {"name":"arr","type":{"type":"array","items":["null","int"]}},
                  {"name":"m","type":{"type":"map","values":["null","int"]}}
                ]}
                """);
    GenericRecord r = new GenericData.Record(schema);
    r.put("lms", 1_000L);
    r.put("ns", -1L);
    r.put("lns", 2_500_000L);
    r.put(
        "fdec",
        new GenericData.Fixed(
            schema.getField("fdec").schema(),
            new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x85}));
    r.put("us", "123e4567-e89b-12d3-a456-426614174000");
    ByteBuffer uuid =
        ByteBuffer.allocate(16).putLong(0x123e4567e89b12d3L).putLong(0xa456426614174000L);
    r.put("uf", new GenericData.Fixed(schema.getField("uf").schema(), uuid.array()));
    r.put("multi", "text");
    r.put("f", 0.5f);
    r.put("nan", Double.NaN);
    r.put("ninf", Double.NEGATIVE_INFINITY);
    List<Integer> items = new ArrayList<>();
    items.add(null);
    items.add(3);
    r.put("arr", items);
    HashMap<String, Integer> map = new HashMap<>();
    map.put("k", null);
    r.put("m", map);
    GenericRecord other = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      other.put(field.name(), r.get(field.name()));
    }
    other.put("multi", 7);
    assertEquals(
        List.of(
            new Ok(
                json(
                    "{\"lms\":1000,\"ns\":-1,\"lns\":2,\"fdec\":\"-0.123\","
                        + "\"us\":\"123e4567-e89b-12d3-a456-426614174000\","
                        + "\"uf\":\"123e4567-e89b-12d3-a456-426614174000\",\"multi\":\"text\",\"f\":0.5,"
                        + "\"nan\":\"NaN\",\"ninf\":\"-Infinity\",\"arr\":[null,3],\"m\":{\"k\":null}}"),
                1,
                null),
            new Ok(
                json(
                    "{\"lms\":1000,\"ns\":-1,\"lns\":2,\"fdec\":\"-0.123\","
                        + "\"us\":\"123e4567-e89b-12d3-a456-426614174000\","
                        + "\"uf\":\"123e4567-e89b-12d3-a456-426614174000\",\"multi\":7,\"f\":0.5,"
                        + "\"nan\":\"NaN\",\"ninf\":\"-Infinity\",\"arr\":[null,3],\"m\":{\"k\":null}}"),
                2,
                null)),
        decode(OCF, ocf(schema, "null", List.of(r, other))));
  }

  @Test
  void arraysOfZeroByteItemsNearTheEndOfABlockDecode() {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":\"null\"}}]}");
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      GenericRecord record = new GenericData.Record(schema);
      List<Object> nulls = new ArrayList<>();
      nulls.add(null);
      nulls.add(null);
      nulls.add(null);
      record.put("a", nulls);
      records.add(record);
    }
    Ok expected = new Ok(json("{\"a\":[null,null,null]}"), 0, null);
    assertEquals(
        List.of(
            new Ok(expected.record(), 1, null),
            new Ok(expected.record(), 2, null),
            new Ok(expected.record(), 3, null)),
        decode(OCF, ocf(schema, "null", records)));
  }

  @Test
  void zeroByteItemsDrawOnOneBudgetPerBlock() throws IOException {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":\"null\"}}]}");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
    for (int i = 0; i < 10; i++) {
      encoder.writeLong(30_000);
    }
    encoder.writeLong(0);
    encoder.flush();
    int length = bytes.size();
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(bytes.toByteArray(), StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, length),
                "MalformedAvroException: array block of 30000 items is more than the block's "
                    + (length + 10_000)
                    + " items left",
                true)),
        decode(datumConfig(schema), bytes.toByteArray()));
  }

  @Test
  void blockClaimingMoreRecordsThanItCanHoldLosesOnlyItself() {
    Schema empty = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}");
    assertEquals(
        List.of(
            malformed(1, "avro block 1 claims 9223372036854775807 records in 0 bytes", false),
            new Ok(json("{}"), 2, null)),
        decode(
            OCF,
            craftedOcf(
                schemaMeta(empty),
                rawBlock(Long.MAX_VALUE, new byte[0]),
                rawBlock(1, new byte[0]))));
  }

  @Test
  void fixedSizeFromTheFileIsCheckedBeforeAllocating() {
    Schema huge =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"H\",\"fields\":[{\"name\":\"f\",\"type\":{\"type\":\"fixed\",\"name\":\"F\",\"size\":1500000000}}]}");
    assertEquals(
        List.of(malformed(1, "fixed of 1500000000 bytes is longer than the 1 bytes left", false)),
        decode(OCF, craftedOcf(schemaMeta(huge), rawBlock(1, new byte[] {1}))));
  }

  @Test
  void deepRecordLosesOnlyItsBlock() {
    Schema list =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"L\",\"fields\":[{\"name\":\"next\",\"type\":[\"null\",\"L\"]}]}");
    byte[] deep = new byte[200_001];
    Arrays.fill(deep, 0, 200_000, (byte) 2);
    assertEquals(
        List.of(
            new Ok(json("{}"), 1, null),
            malformed(2, "avro record nests too deeply for the thread stack", false),
            new Ok(json("{}"), 3, null)),
        decode(
            OCF,
            craftedOcf(
                schemaMeta(list),
                rawBlock(1, new byte[] {0}),
                rawBlock(1, deep),
                rawBlock(1, new byte[] {0}))));
  }

  @Test
  void recordThatDecodesButDoesNotConvertLosesOnlyItself() {
    Schema decimal =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"d\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}");
    List<GenericRecord> records = new ArrayList<>();
    for (byte[] value : List.of(new byte[] {1}, new byte[0], new byte[] {2})) {
      GenericRecord record = new GenericData.Record(decimal);
      record.put("d", ByteBuffer.wrap(value));
      records.add(record);
    }
    assertEquals(
        List.of(
            new Ok(json("{\"d\":\"0.01\"}"), 1, null),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "NumberFormatException: Zero length BigInteger",
                false),
            new Ok(json("{\"d\":\"0.02\"}"), 3, null)),
        decode(OCF, ocf(decimal, "null", records)));
  }

  @Test
  void bytesLeftAfterABlocksRecordsAreReportedAgainstTheBlock() {
    assertEquals(
        List.of(
            smallRecord("a", 1),
            malformed(1, "avro block 1 has 1 bytes left after its records", false),
            smallRecord("b", 2)),
        decode(
            OCF,
            craftedOcf(
                schemaMeta(SMALL),
                rawBlock(1, concat(datum(small("a")), new byte[] {0})),
                rawBlock(1, datum(small("b"))))));
  }

  @Test
  void xzBlockDecodesUnderASmallDocumentLimit() {
    assertEquals(
        List.of(smallRecord("a", 1)),
        decode(
            decoder(OCF),
            ByteInput.ofBytes(ocf(SMALL, "xz", List.of(small("a")))),
            new DecodeLimits(0, 1 << 20)));
  }

  @Test
  void snappyBlockChecksumIsVerified() {
    byte[] file = ocf(SMALL, "snappy", List.of(small("abc")));
    file[file.length - 17] ^= 1;
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                1,
                null,
                "CorruptBlockException: avro snappy block checksum mismatch",
                false)),
        decode(OCF, file));
  }

  @Test
  void headerProblemsAreTerminal() {
    String invalidSchemaMessage;
    try {
      new Schema.Parser().parse("{\"type\":");
      throw new AssertionError("expected a parse failure");
    } catch (RuntimeException e) {
      invalidSchemaMessage = e.getMessage().lines().findFirst().orElse("");
    }
    assertEquals(
        List.of(malformed(1, "avro file header has no schema", true)),
        decode(OCF, craftedOcf(Map.of("x", "y"))));
    assertEquals(
        List.of(malformed(1, "avro file schema is a STRING, not a record", true)),
        decode(OCF, craftedOcf(Map.of("avro.schema", "\"string\""))));
    assertEquals(
        List.of(malformed(1, "avro codec lzo is not supported", true)),
        decode(OCF, craftedOcf(Map.of("avro.schema", SMALL.toString(), "avro.codec", "lzo"))));
    assertEquals(
        List.of(malformed(1, "avro file schema is invalid: " + invalidSchemaMessage, true)),
        decode(OCF, craftedOcf(Map.of("avro.schema", "{\"type\":"))));
  }

  @Test
  void headerEntryCountsAndSizesAreBounded() throws IOException {
    ByteArrayOutputStream badCount = new ByteArrayOutputStream();
    badCount.write(new byte[] {'O', 'b', 'j', 1});
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(badCount, null);
    encoder.writeLong(Long.MIN_VALUE);
    encoder.writeLong(0);
    encoder.flush();
    assertEquals(
        List.of(malformed(1, "avro file header has an invalid entry count", true)),
        decode(OCF, badCount.toByteArray()));
    int entries = 9_000_000;
    ByteArrayOutputStream endless = new ByteArrayOutputStream();
    endless.write(new byte[] {'O', 'b', 'j', 1});
    encoder = EncoderFactory.get().binaryEncoder(endless, null);
    encoder.writeLong(entries);
    encoder.flush();
    endless.write(new byte[2 * entries]);
    assertEquals(
        List.of(malformed(1, "avro file header is longer than 16777216 bytes", true)),
        decode(OCF, endless.toByteArray()));
  }

  @Test
  void negativeBlockCountIsTerminal() {
    assertEquals(
        List.of(malformed(1, "avro block 1 has a negative count or size", true)),
        decode(OCF, craftedOcf(schemaMeta(SMALL), rawBlock(-1, new byte[0]))));
  }

  static Stream<String> compressedCodecs() {
    return codecs().filter(codec -> !codec.equals("null"));
  }

  @ParameterizedTest
  @MethodSource("compressedCodecs")
  void blockThatDecompressesOverTheLimitIsOversizedForEveryCodec(String codec) {
    byte[] file = ocf(SMALL, codec, List.of(small("x".repeat(100_000))), List.of(small("b")));
    List<Object> actual = decode(decoder(OCF), ByteInput.ofBytes(file), new DecodeLimits(0, 5_000));
    long size = ((Big) actual.get(0)).sizeSoFar();
    assertTrue(size > 5_000 && size <= 5_000 + 64 * 1024, codec + " size " + size);
    assertEquals(List.of(new Big(1, null, size, "", false), smallRecord("b", 2)), actual, codec);
  }

  @Test
  void datumOverTheDocumentLimitIsOversized() {
    byte[] input = datum(small("x".repeat(100)));
    assertEquals(
        List.of(
            new Big(
                -1,
                RawSpan.toEnd(0),
                input.length,
                new String(input, StandardCharsets.UTF_8),
                true)),
        decode(decoder(datumConfig(SMALL)), ByteInput.ofBytes(input), new DecodeLimits(0, 50)));
  }

  @ParameterizedTest
  @MethodSource("compressedCodecs")
  void compressedBlockMayHoldFarMoreRecordsThanCompressedBytes(String codec) throws IOException {
    Schema flag =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"b\",\"type\":\"boolean\"}]}");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(flag))) {
      writer.setCodec(codec(codec));
      writer.setSyncInterval(1 << 20);
      writer.create(flag, bytes);
      GenericRecord record = new GenericData.Record(flag);
      record.put("b", true);
      for (int i = 0; i < 300_000; i++) {
        writer.append(record);
      }
    }
    List<Object> expected = new ArrayList<>();
    for (int i = 1; i <= 300_000; i++) {
      expected.add(new Ok(json("{\"b\":true}"), i, null));
    }
    assertEquals(expected, decode(OCF, bytes.toByteArray()), codec);
  }

  @Test
  void wideRecordsOfFieldsThatEncodeInNoBytesAreBounded() throws IOException {
    StringBuilder fields = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      fields
          .append(i == 0 ? "" : ",")
          .append("{\"name\":\"n")
          .append(i)
          .append("\",\"type\":\"null\"}");
    }
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"W\",\"fields\":[{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":"
                    + "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + fields
                    + "]}}}]}");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
    encoder.writeLong(100_000);
    encoder.writeLong(0);
    encoder.flush();
    byte[] input = bytes.toByteArray();
    // The outer record costs its one field plus itself, then the array claims 100,000 items, and
    // each inner record costs its 100 fields plus itself.
    long budget = 16L * input.length + 1_000_000 - 2 - 100_000;
    long left = budget % 101;
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(input, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, input.length),
                "MalformedAvroException: record R would build more objects than the block's "
                    + left
                    + " left",
                true)),
        decode(datumConfig(schema), input));
  }

  @Test
  void recordsWithSeveralNullColumnsDecode() throws IOException {
    StringBuilder fields = new StringBuilder("{\"name\":\"id\",\"type\":\"int\"}");
    for (int i = 0; i < 10; i++) {
      fields.append(",{\"name\":\"n").append(i).append("\",\"type\":\"null\"}");
    }
    fields.append(",{\"name\":\"s\",\"type\":[\"null\",\"string\"]}");
    Schema schema =
        new Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"N\",\"fields\":[" + fields + "]}");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.setSyncInterval(1 << 20);
      writer.create(schema, bytes);
      for (int i = 0; i < 100_000; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", i);
        writer.append(record);
      }
    }
    List<Object> expected = new ArrayList<>();
    for (int i = 0; i < 100_000; i++) {
      expected.add(new Ok(json("{\"id\":" + i + "}"), i + 1, null));
    }
    assertEquals(expected, decode(OCF, bytes.toByteArray()));
  }

  @Test
  void moreZeroByteRecordsThanTheAllowanceLoseTheirBlock() throws IOException {
    Schema empty = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"E\",\"fields\":[]}");
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(empty))) {
      writer.create(empty, bytes);
      GenericRecord record = new GenericData.Record(empty);
      for (int i = 0; i < 200_000; i++) {
        writer.append(record);
      }
    }
    assertEquals(
        List.of(malformed(1, "avro block 1 claims 200000 records in 0 bytes", false)),
        decode(OCF, bytes.toByteArray()));
  }

  @Test
  void zstdBlockWithTooLargeAWindowIsOversized() {
    byte[] hugeWindow = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, (byte) 0xA0, 1, 0, 0};
    assertEquals(
        List.of(new Big(1, null, 0, "", false), smallRecord("b", 2)),
        decode(
            OCF,
            craftedOcf(
                Map.of("avro.schema", SMALL.toString(), "avro.codec", "zstandard"),
                rawBlock(1, hugeWindow),
                rawBlock(1, Zstd.compress(datum(small("b")))))));
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
