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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

class ParquetDecodeTest {

  private static final String PARQUET = "{\"format\":{\"type\":\"parquet\"}}";

  private static final MessageType EVERY_TYPE =
      MessageTypeParser.parseMessageType(
          """
          message every {
            required boolean flag;
            required int32 i32;
            required int64 i64;
            required float f;
            required double d;
            optional int32 missing;
            required binary s (STRING);
            required binary e (ENUM);
            required binary j (JSON);
            required binary raw;
            required fixed_len_byte_array(16) id (UUID);
            required int32 dec32 (DECIMAL(9,2));
            required int64 dec64 (DECIMAL(18,3));
            required binary decBin (DECIMAL(20,2));
            required int32 day (DATE);
            required int32 tMillis (TIME(MILLIS,true));
            required int64 tMicros (TIME(MICROS,true));
            required int64 tsMillis (TIMESTAMP(MILLIS,true));
            required int64 tsMicros (TIMESTAMP(MICROS,true));
            required int64 tsNanos (TIMESTAMP(NANOS,false));
            required int96 legacy;
            required int32 u8 (INTEGER(8,false));
            required int32 u32 (INTEGER(32,false));
            required int64 u64 (INTEGER(64,false));
            optional group tags (LIST) {
              repeated group list {
                optional binary element (STRING);
              }
            }
            optional group attrs (MAP) {
              repeated group key_value {
                required binary key (STRING);
                optional int32 value;
              }
            }
            optional group byId (MAP) {
              repeated group key_value {
                required int64 key;
                required binary value (STRING);
              }
            }
            optional group old (LIST) {
              repeated int32 array;
            }
            repeated int32 nums;
            optional group inner {
              required int32 a;
              optional binary b (STRING);
            }
          }
          """);

  private static Group everyType(SimpleGroupFactory rows, UUID id) {
    Group row =
        rows.newGroup()
            .append("flag", true)
            .append("i32", -7)
            .append("i64", 1L << 40)
            .append("f", Float.NaN)
            .append("d", 1.5)
            .append("s", "héllo")
            .append("e", "RED")
            .append("j", "{\"a\":1}")
            .append("raw", Binary.fromConstantByteArray(new byte[] {1, 2, 3}))
            .append(
                "id",
                Binary.fromConstantByteArray(
                    ByteBuffer.allocate(16)
                        .putLong(id.getMostSignificantBits())
                        .putLong(id.getLeastSignificantBits())
                        .array()))
            .append("dec32", 12345)
            .append("dec64", -1L)
            .append(
                "decBin",
                Binary.fromConstantByteArray(new BigInteger("-12345678901234567890").toByteArray()))
            .append("day", 19000)
            .append("tMillis", 3_600_000)
            .append("tMicros", 3_600_000_001L)
            .append("tsMillis", 1_700_000_000_123L)
            .append("tsMicros", -1L)
            .append("tsNanos", 1_700_000_000_123_456_789L)
            .append("legacy", new NanoTime(2_440_589, 1_000_000_123L))
            .append("u8", 255)
            .append("u32", -1)
            .append("u64", -1L);
    Group tags = row.addGroup("tags");
    tags.addGroup("list").append("element", "a");
    tags.addGroup("list");
    tags.addGroup("list").append("element", "b");
    Group attrs = row.addGroup("attrs");
    attrs.addGroup("key_value").append("key", "k").append("value", 1);
    attrs.addGroup("key_value").append("key", "n");
    row.addGroup("byId").addGroup("key_value").append("key", 7L).append("value", "x");
    row.addGroup("old").append("array", 1).append("array", 2);
    row.addGroup("inner").append("a", 3);
    return row;
  }

  private static String everyTypeJson(UUID id) {
    return "{\"flag\":true,\"i32\":-7,\"i64\":1099511627776,\"f\":\"NaN\",\"d\":1.5,"
        + "\"s\":\"héllo\",\"e\":\"RED\",\"j\":\"{\\\"a\\\":1}\",\"raw\":\"AQID\","
        + "\"id\":\""
        + id
        + "\",\"dec32\":\"123.45\",\"dec64\":\"-0.001\","
        + "\"decBin\":\"-123456789012345678.90\",\"day\":19000,\"tMillis\":3600000,"
        + "\"tMicros\":3600000,\"tsMillis\":1700000000123,\"tsMicros\":-1,"
        + "\"tsNanos\":1700000000123,\"legacy\":86401000,\"u8\":255,\"u32\":4294967295,"
        + "\"u64\":1.8446744073709552E19,\"tags\":[\"a\",null,\"b\"],"
        + "\"attrs\":{\"k\":1,\"n\":null},\"byId\":{\"7\":\"x\"},\"old\":[1,2],\"nums\":[],"
        + "\"inner\":{\"a\":3}}";
  }

  @Test
  void everyTypeMaps() {
    UUID id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    byte[] file =
        ParquetTestFiles.write(
            EVERY_TYPE, List.of(everyType(new SimpleGroupFactory(EVERY_TYPE), id)), options -> {});
    assertEquals(List.of(new Ok(json(everyTypeJson(id)), 1, null)), decode(PARQUET, file));
  }

  @Test
  void everyCodecAndPageVersionDecodes() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message m { required int64 n; optional binary s (STRING); required boolean b; }");
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    List<Group> written = new ArrayList<>();
    List<Object> expected = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      Group row = rows.newGroup().append("n", (long) i * 3).append("b", i % 3 == 0);
      if (i % 5 != 0) {
        row.append("s", "v" + (i % 7));
      }
      written.add(row);
      expected.add(
          new Ok(
              json(
                  "{\"n\":"
                      + i * 3
                      + (i % 5 != 0 ? ",\"s\":\"v" + (i % 7) + "\"" : "")
                      + ",\"b\":"
                      + (i % 3 == 0)
                      + "}"),
              i + 1,
              null));
    }
    for (CompressionCodecName codec :
        List.of(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            CompressionCodecName.ZSTD,
            CompressionCodecName.LZ4)) {
      for (WriterVersion version : WriterVersion.values()) {
        for (boolean dictionary : List.of(true, false)) {
          byte[] file =
              ParquetTestFiles.write(
                  schema,
                  written,
                  options ->
                      options
                          .withCompressionCodec(codec)
                          .withWriterVersion(version)
                          .withDictionaryEncoding(dictionary)
                          .withPageSize(4096)
                          .withRowGroupSize(16 * 1024));
          assertEquals(expected, decode(PARQUET, file), codec + " " + version + " " + dictionary);
        }
      }
    }
  }

  private static final MessageType NUMBERS =
      MessageTypeParser.parseMessageType("message m { required int64 n; }");

  /** {@code count} rows of n = 0, 1, ..., in row groups of about {@code rowGroupBytes}. */
  private static byte[] numbers(int count, int rowGroupBytes, CompressionCodecName codec) {
    SimpleGroupFactory rows = new SimpleGroupFactory(NUMBERS);
    List<Group> written =
        IntStream.range(0, count).mapToObj(i -> rows.newGroup().append("n", (long) i)).toList();
    return ParquetTestFiles.write(
        NUMBERS,
        written,
        options ->
            options
                .withCompressionCodec(codec)
                .withDictionaryEncoding(false)
                .withPageSize(1024)
                .withRowGroupSize(rowGroupBytes));
  }

  private static List<Object> numbersDecoded(int from, int to) {
    return IntStream.range(from, to)
        .mapToObj(i -> (Object) new Ok(json("{\"n\":" + i + "}"), i + 1, null))
        .toList();
  }

  static FileMetaData footer(byte[] file) {
    int length = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    FileMetaData metadata = new FileMetaData();
    try {
      metadata.read(
          new TCompactProtocol(
              new TIOStreamTransport(
                  new ByteArrayInputStream(file, file.length - 8 - length, length))));
    } catch (TException e) {
      throw new IllegalStateException(e);
    }
    return metadata;
  }

  /** The row group's stored size, and what its pages claim to decompress to. */
  private static long[] sizes(byte[] file, RowGroup group) {
    long stored = 0;
    long decompressed = 0;
    for (var chunk : group.getColumns()) {
      var meta = chunk.getMeta_data();
      long start =
          meta.isSetDictionary_page_offset() && meta.getDictionary_page_offset() > 0
              ? Math.min(meta.getDictionary_page_offset(), meta.getData_page_offset())
              : meta.getData_page_offset();
      stored += meta.getTotal_compressed_size();
      ByteArrayInputStream in =
          new ByteArrayInputStream(file, (int) start, (int) meta.getTotal_compressed_size());
      while (in.available() > 0) {
        PageHeader header = new PageHeader();
        try {
          header.read(new TCompactProtocol(new TIOStreamTransport(in)));
        } catch (TException e) {
          throw new IllegalStateException(e);
        }
        decompressed += header.getUncompressed_page_size();
        in.skip(header.getCompressed_page_size());
      }
    }
    return new long[] {stored, decompressed};
  }

  @Test
  void rowGroupsFollowEachOtherAndOneOverTheDocumentLimitIsSkipped() {
    byte[] file = numbers(5000, 8 * 1024, CompressionCodecName.UNCOMPRESSED);
    List<RowGroup> groups = footer(file).getRow_groups();
    assertEquals(true, groups.size() >= 3, "row groups: " + groups.size());
    assertEquals(numbersDecoded(0, 5000), decode(PARQUET, file));
    long[] first = sizes(file, groups.get(0));
    long largest = groups.stream().mapToLong(g -> sizes(file, g)[0]).max().orElseThrow();
    assertEquals(
        numbersDecoded(0, 5000),
        decode(decoder(PARQUET), ByteInput.ofBytes(file), new DecodeLimits(0, largest)));
    List<Object> expected = new ArrayList<>();
    long index = 1;
    for (RowGroup group : groups) {
      long[] size = sizes(file, group);
      if (size[0] > largest - 1) {
        expected.add(new Big(index, null, size[0], "", false));
      } else {
        expected.addAll(numbersDecoded((int) index - 1, (int) (index - 1 + group.getNum_rows())));
      }
      index += group.getNum_rows();
    }
    assertEquals(
        expected,
        decode(decoder(PARQUET), ByteInput.ofBytes(file), new DecodeLimits(0, largest - 1)));
    assertEquals(true, first[0] > first[1], "uncompressed pages are smaller than their chunk");
  }

  @Test
  void rowGroupOverTheDocumentLimitOnceDecompressedIsSkipped() {
    byte[] file = numbers(3000, 1024 * 1024, CompressionCodecName.GZIP);
    RowGroup group = footer(file).getRow_groups().get(0);
    long[] size = sizes(file, group);
    assertEquals(true, size[1] > size[0], "gzip pages decompress to more than they take");
    assertEquals(
        numbersDecoded(0, 3000),
        decode(decoder(PARQUET), ByteInput.ofBytes(file), new DecodeLimits(0, size[1])));
    assertEquals(
        List.of(new Big(1, null, size[1], "", false)),
        decode(decoder(PARQUET), ByteInput.ofBytes(file), new DecodeLimits(0, size[1] - 1)));
  }

  @Test
  void rowOverTheRecordLimitLosesOnlyItself() {
    MessageType schema =
        MessageTypeParser.parseMessageType("message m { required binary s (STRING); }");
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    byte[] file =
        ParquetTestFiles.write(
            schema,
            List.of(
                rows.newGroup().append("s", "a"),
                rows.newGroup().append("s", "x".repeat(100)),
                rows.newGroup().append("s", "b")),
            options -> {});
    String big = "{\"s\":\"" + "x".repeat(100) + "\"}";
    assertEquals(
        List.of(
            new Ok(json("{\"s\":\"a\"}"), 1, null),
            new Ok(json(big), 2, null),
            new Ok(json("{\"s\":\"b\"}"), 3, null)),
        decode(decoder(PARQUET), ByteInput.ofBytes(file), new DecodeLimits(116, 0)));
    assertEquals(
        List.of(
            new Ok(json("{\"s\":\"a\"}"), 1, null),
            new Big(2, null, 116, "", false),
            new Ok(json("{\"s\":\"b\"}"), 3, null)),
        decode(decoder(PARQUET), ByteInput.ofBytes(file), new DecodeLimits(115, 0)));
  }

  @Test
  void parquetIsCompiledWithoutCompressionAndNeedsRandomAccess() {
    for (String compression : List.of("[\"gzip\"]", "[\"auto\"]")) {
      String config = "{\"compression\":" + compression + ",\"format\":{\"type\":\"parquet\"}}";
      assertEquals(
          "parquet cannot be combined with compression; its pages carry their own",
          assertThrows(IllegalArgumentException.class, () -> decoder(config)).getMessage());
    }
    byte[] file = numbers(3, 1024 * 1024, CompressionCodecName.UNCOMPRESSED);
    ByteInput streamOnly = () -> new ByteArrayInputStream(file);
    assertEquals(
        "parquet needs an input with ByteInput.randomAccess()",
        assertThrows(
                IllegalStateException.class,
                () -> decoder(PARQUET).open(streamOnly, DecodeLimits.UNLIMITED))
            .getMessage());
    assertEquals(
        new DecodeResult(
            List.of(json("{\"n\":0}"), json("{\"n\":1}"), json("{\"n\":2}")), List.of()),
        decoder(PARQUET).decodeAll(file, DecodeLimits.UNLIMITED));
  }

  /** Random access over {@code file} that records every range asked for. */
  private record Recording(byte[] file, List<long[]> requests) implements RandomAccessInput {
    @Override
    public long length() {
      return file.length;
    }

    @Override
    public InputStream read(long offset, long length) {
      requests.add(new long[] {offset, length});
      return new ByteArrayInputStream(file, (int) offset, (int) length);
    }
  }

  private static ByteInput ranges(RandomAccessInput ranges) {
    return new ByteInput() {
      @Override
      public InputStream openStream() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Optional<RandomAccessInput> randomAccess() {
        return Optional.of(ranges);
      }
    };
  }

  @Test
  void eachByteIsRequestedOnceInBoundedRanges() {
    byte[] file = numbers(5000, 8 * 1024, CompressionCodecName.SNAPPY);
    Recording recording = new Recording(file, new ArrayList<>());
    assertEquals(
        numbersDecoded(0, 5000),
        decode(decoder(PARQUET), ranges(recording), DecodeLimits.UNLIMITED));
    FileMetaData metadata = footer(file);
    int footerLength =
        ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    List<List<Long>> expected = new ArrayList<>();
    expected.add(List.of(0L, 4L));
    expected.add(List.of(file.length - 8L, 8L));
    expected.add(List.of(file.length - 8L - footerLength, (long) footerLength));
    for (RowGroup group : metadata.getRow_groups()) {
      for (var chunk : group.getColumns()) {
        expected.add(
            List.of(
                chunk.getMeta_data().getData_page_offset(),
                chunk.getMeta_data().getTotal_compressed_size()));
      }
    }
    assertEquals(expected, recording.requests().stream().map(r -> List.of(r[0], r[1])).toList());
  }

  @Test
  void readFailuresThrowWhereverTheyHappen() {
    byte[] file = numbers(2000, 8 * 1024, CompressionCodecName.UNCOMPRESSED);
    Recording count = new Recording(file, new ArrayList<>());
    decode(decoder(PARQUET), ranges(count), DecodeLimits.UNLIMITED);
    for (int failAt = 0; failAt <= count.requests().size(); failAt++) {
      int fail = failAt;
      int[] calls = {0};
      RandomAccessInput failing =
          new RandomAccessInput() {
            @Override
            public long length() throws IOException {
              if (fail == 0) {
                throw new IOException("length lost");
              }
              return file.length;
            }

            @Override
            public InputStream read(long offset, long length) throws IOException {
              if (++calls[0] == fail) {
                throw new IOException("read lost");
              }
              if (calls[0] == fail - 1 && length > 1) {
                return new ByteArrayInputStream(file, (int) offset, (int) length - 1);
              }
              return new ByteArrayInputStream(file, (int) offset, (int) length);
            }
          };
      List<Object> outcomes = decode(decoder(PARQUET), ranges(failing), DecodeLimits.UNLIMITED);
      assertEquals(
          new Threw("SourceReadException"), outcomes.get(outcomes.size() - 1), "fail at " + fail);
    }
  }

  @Test
  void sparseWideRowGroupsDecode() {
    StringBuilder schema = new StringBuilder("message wide {");
    for (int i = 0; i < 100; i++) {
      schema.append(" optional int64 c").append(i).append(';');
    }
    MessageType wide = MessageTypeParser.parseMessageType(schema.append(" }").toString());
    SimpleGroupFactory rows = new SimpleGroupFactory(wide);
    List<Group> written =
        IntStream.range(0, 50_000).mapToObj(i -> rows.newGroup().append("c0", (long) i)).toList();
    byte[] file =
        ParquetTestFiles.write(
            wide,
            written,
            options ->
                options.withWriterVersion(WriterVersion.PARQUET_2_0).withDictionaryEncoding(false));
    assertEquals(
        IntStream.range(0, 50_000)
            .mapToObj(i -> (Object) new Ok(json("{\"c0\":" + i + "}"), i + 1, null))
            .toList(),
        decode(PARQUET, file));
  }
}
