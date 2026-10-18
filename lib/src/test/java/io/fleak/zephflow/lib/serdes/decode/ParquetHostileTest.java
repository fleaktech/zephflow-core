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

import io.fleak.zephflow.lib.serdes.decode.ParquetTestFiles.RawChunk;
import io.fleak.zephflow.lib.serdes.decode.ParquetTestFiles.RawPage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import net.jpountz.lz4.LZ4Factory;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

/** Files that lie about their sizes and counts; none may cost more than its bytes. */
class ParquetHostileTest {

  private static final String PARQUET = "{\"format\":{\"type\":\"parquet\"}}";

  static byte[] ints(int... values) {
    ByteBuffer buffer = ByteBuffer.allocate(4 * values.length).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      buffer.putInt(value);
    }
    return buffer.array();
  }

  static RawPage page(int values, Encoding encoding, byte[] data) {
    return page(values, encoding, data, data.length);
  }

  static RawPage page(int values, Encoding encoding, byte[] data, int uncompressed) {
    return new RawPage(
        new PageHeader(PageType.DATA_PAGE, uncompressed, data.length)
            .setData_page_header(new DataPageHeader(values, encoding, Encoding.RLE, Encoding.RLE)),
        data);
  }

  static RawChunk column(long values, RawPage... pages) {
    return new RawChunk("n", Type.INT32, CompressionCodec.UNCOMPRESSED, values, List.of(pages));
  }

  static final RawChunk VALID = column(3, page(3, Encoding.PLAIN, ints(1, 2, 3)));

  /** Three required int32 rows 1, 2, 3, as a hostile row group and then the same one intact. */
  static byte[] hostileThenIntact(RawChunk chunk, UnaryOperator<RowGroup> hostile) {
    return hostileThenIntact(FieldRepetitionType.REQUIRED, chunk, VALID, hostile);
  }

  static byte[] hostileThenIntact(
      FieldRepetitionType repetition,
      RawChunk chunk,
      RawChunk valid,
      UnaryOperator<RowGroup> hostile) {
    return ParquetTestFiles.assemble(
        repetition,
        3,
        List.of(List.of(chunk), List.of(valid)),
        footer ->
            footer.setRow_groups(
                List.of(
                    hostile.apply(footer.getRow_groups().get(0)), footer.getRow_groups().get(1))));
  }

  static List<Object> lostThenIntact(String error, long intactFrom) {
    List<Object> expected = new ArrayList<>();
    expected.add(new Bad(ErrorKind.MALFORMED, "", 1, null, error, false));
    for (int i = 1; i <= 3; i++) {
      expected.add(new Ok(json("{\"n\":" + i + "}"), intactFrom + i - 1, null));
    }
    return expected;
  }

  @Test
  void assembledFileDecodes() {
    assertEquals(SIX_ROWS, decode(PARQUET, hostileThenIntact(VALID, group -> group)));
  }

  private static Bad terminal(String error) {
    return new Bad(ErrorKind.MALFORMED, "", 1, null, error, true);
  }

  @Test
  void brokenFilesAreTerminal() {
    byte[] file = hostileThenIntact(VALID, group -> group);
    byte[] wrongStart = file.clone();
    wrongStart[0] = 'X';
    byte[] footerTooLong = file.clone();
    ByteBuffer.wrap(footerTooLong, file.length - 8, 4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(file.length);
    assertEquals(
        List.of(
            terminal("MalformedParquetException: not a parquet file: it does not start with PAR1")),
        decode(PARQUET, wrongStart));
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: not a parquet file, or a cut-off one: it does not end with PAR1")),
        decode(PARQUET, Arrays.copyOf(file, file.length - 1)));
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: not a parquet file: 11 bytes is too short for the magic and footer")),
        decode(PARQUET, Arrays.copyOf(file, 11)));
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: parquet footer of "
                    + file.length
                    + " bytes does not fit in a file of "
                    + file.length)),
        decode(PARQUET, footerTooLong));
  }

  private static final long PINNED_FOOTER = 4076;

  private static final List<Object> SIX_ROWS =
      List.of(
          new Ok(json("{\"n\":1}"), 1, null),
          new Ok(json("{\"n\":2}"), 2, null),
          new Ok(json("{\"n\":3}"), 3, null),
          new Ok(json("{\"n\":1}"), 4, null),
          new Ok(json("{\"n\":2}"), 5, null),
          new Ok(json("{\"n\":3}"), 6, null));

  private static List<Object> decodeWithin(byte[] file, long maxDocumentBytes) {
    return ParquetDecodeTest.decodeWithin(file, maxDocumentBytes);
  }

  @Test
  void footerOverTheLimitIsTerminalAndUnreadWhenItsBytesAreOver() {
    byte[] file = hostileThenIntact(VALID, group -> group);
    int length = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    long estimate = ParquetDecodeTest.footerEstimate(file);
    assertTrue(estimate > length + 2048, "the footer holds more than its bytes");
    List<long[]> requests = new ArrayList<>();
    RandomAccessInput recording =
        new RandomAccessInput() {
          @Override
          public long length() {
            return file.length;
          }

          @Override
          public InputStream read(long offset, long count) {
            requests.add(new long[] {offset, count});
            return new ByteArrayInputStream(file, (int) offset, (int) count);
          }
        };
    assertEquals(
        List.of(new Big(1, null, length, "", true)),
        decode(
            decoder(PARQUET),
            new ByteInput() {
              @Override
              public InputStream openStream() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Optional<RandomAccessInput> randomAccess() {
                return Optional.of(recording);
              }
            },
            new DecodeLimits(0, length - 1)));
    assertEquals(
        List.of(List.of(0L, 4L), List.of(file.length - 8L, 8L)),
        requests.stream().map(r -> List.of(r[0], r[1])).toList());
    assertEquals(List.of(new Big(1, null, estimate, "", true)), decodeWithin(file, estimate - 1));
    assertEquals(SIX_ROWS, decodeWithin(file, estimate));
  }

  private static byte[] withRawFooter(byte... footer) {
    return ParquetTestFiles.withFooter(ParquetTestFiles.magic(), footer);
  }

  @Test
  void footerCountsAndNestingAreBoundedByItsBytes() {
    ByteArrayOutputStream list = new ByteArrayOutputStream();
    list.writeBytes(new byte[] {0x15, 0x02, 0x19, (byte) 0xFC});
    list.writeBytes(new byte[] {(byte) 0x80, (byte) 0xE1, (byte) 0xEB, 0x17});
    ByteArrayOutputStream deep = new ByteArrayOutputStream();
    for (int i = 0; i < 100; i++) {
      deep.writeBytes(new byte[] {0x0C, (byte) 0xC8, 0x01});
    }
    deep.writeBytes(new byte[101]);
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: parquet FileMetaData does not parse: Length exceeded max allowed: 50000000")),
        decode(PARQUET, withRawFooter(list.toByteArray())));
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: parquet FileMetaData does not parse: structures nest deeper than 64 levels")),
        decode(PARQUET, withRawFooter(deep.toByteArray())));
  }

  private static byte[] withSchema(List<SchemaElement> schema) {
    return ParquetTestFiles.assemble(
        FieldRepetitionType.REQUIRED,
        3,
        List.of(List.of(VALID)),
        footer -> footer.setSchema(schema));
  }

  @Test
  void schemasThatParquetCouldNotBuildSafelyAreRejected() {
    List<SchemaElement> deep = new ArrayList<>();
    deep.add(new SchemaElement("schema").setNum_children(1));
    for (int i = 0; i < 40; i++) {
      deep.add(
          new SchemaElement("g" + i)
              .setNum_children(1)
              .setRepetition_type(FieldRepetitionType.OPTIONAL));
    }
    deep.add(
        new SchemaElement("n")
            .setType(Type.INT32)
            .setRepetition_type(FieldRepetitionType.REQUIRED));
    List<SchemaElement> overflowing =
        List.of(
            new SchemaElement("schema").setNum_children(1),
            new SchemaElement("n")
                .setType(Type.INT32)
                .setRepetition_type(FieldRepetitionType.REQUIRED),
            new SchemaElement("extra")
                .setType(Type.INT32)
                .setRepetition_type(FieldRepetitionType.REQUIRED));
    List<SchemaElement> unfinished =
        List.of(
            new SchemaElement("schema").setNum_children(2),
            new SchemaElement("n")
                .setType(Type.INT32)
                .setRepetition_type(FieldRepetitionType.REQUIRED));
    List<SchemaElement> empty = List.of(new SchemaElement("schema").setNum_children(0));
    assertEquals(
        List.of(terminal("MalformedParquetException: parquet schema nests deeper than 32 levels")),
        decode(PARQUET, withSchema(deep)));
    assertEquals(
        List.of(
            terminal("MalformedParquetException: parquet schema has 1 elements outside its root")),
        decode(PARQUET, withSchema(overflowing)));
    assertEquals(
        List.of(terminal("MalformedParquetException: parquet schema ends inside a group")),
        decode(PARQUET, withSchema(unfinished)));
    assertEquals(
        List.of(terminal("MalformedParquetException: parquet schema has no columns")),
        decode(PARQUET, withSchema(empty)));
  }

  @Test
  void columnChunksOutsideTheDataOrWithWrongCountsLoseTheirRowGroup() {
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet column chunk [n] of 1000000 bytes at 4 is outside the data",
            4),
        decode(
            PARQUET,
            hostileThenIntact(
                VALID,
                group -> {
                  group.getColumns().get(0).getMeta_data().setTotal_compressed_size(1_000_000);
                  return group;
                })));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet column chunk ends after 3 of 4 values", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                VALID,
                group -> {
                  group.getColumns().get(0).getMeta_data().setNum_values(4);
                  return group;
                })));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet row group has 2 column chunks for 1 columns", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                VALID,
                group ->
                    group.setColumns(
                        List.of(group.getColumns().get(0), group.getColumns().get(0))))));
  }

  @Test
  void pagesThatClaimMoreThanTheyHoldLoseTheirRowGroup() {
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet page of 13 bytes does not fit its column chunk", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(
                    3,
                    new RawPage(
                        new PageHeader(PageType.DATA_PAGE, 12, 13)
                            .setData_page_header(
                                new DataPageHeader(3, Encoding.PLAIN, Encoding.RLE, Encoding.RLE)),
                        ints(1, 2, 3))),
                group -> group)));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet column chunk [n] has 2000000 values for 3 rows", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(2_000_000, page(2_000_000, Encoding.PLAIN, ints(1, 2, 3))),
                group -> group)));
    assertEquals(
        lostThenIntact("MalformedParquetException: uncompressed page of 12 bytes claims 1000", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, page(3, Encoding.PLAIN, ints(1, 2, 3), 1000)), group -> group)));
  }

  static byte[] concatAll(byte[]... parts) {
    return concat(parts);
  }

  /** Optional int32 rows 1, 2, 3: definition levels as one run of three, then the values. */
  static final RawChunk OPTIONAL_VALID =
      column(3, page(3, Encoding.PLAIN, concatAll(new byte[] {2, 0, 0, 0, 6, 1}, ints(1, 2, 3))));

  @Test
  void runsThatClaimMoreValuesThanThePageHasLoseTheirRowGroup() {
    byte[] bomb = {6, 0, 0, 0, (byte) 0x81, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01, 0};
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet bit-packed run of 1073741824 values does not fit the page",
            4),
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.OPTIONAL,
                column(3, page(3, Encoding.PLAIN, concatAll(bomb, ints(1, 2, 3)))),
                OPTIONAL_VALID,
                group -> group)));
  }

  @Test
  void dictionaryThatClaimsMoreEntriesThanItsBytesLosesItsRowGroup() {
    RawPage dictionary =
        new RawPage(
            new PageHeader(PageType.DICTIONARY_PAGE, 4, 4)
                .setDictionary_page_header(new DictionaryPageHeader(1000, Encoding.PLAIN)),
            ints(9));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet dictionary of 1000 entries does not fit in 4 bytes",
            4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, dictionary, page(3, Encoding.RLE_DICTIONARY, new byte[] {1, 6, 0})),
                group -> group)));
  }

  @Test
  void deltaHeadersThatClaimTooMuchLoseTheirRowGroup() {
    byte[] tooManyValues = {
      (byte) 0x80, 0x01, 0x04, (byte) 0x80, (byte) 0x94, (byte) 0xEB, (byte) 0xDC, 0x03, 0x02
    };
    byte[] hugeBlock = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x04, 0x04, 0x03, 0x02};
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet delta encoding claims 1000000000 values in a page of 3",
            4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, page(3, Encoding.DELTA_BINARY_PACKED, tooManyValues)), group -> group)));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet delta block of 1073741824 values in 4 miniblocks",
            4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, page(3, Encoding.DELTA_BINARY_PACKED, hugeBlock)), group -> group)));
  }

  private static byte[] deltaInts(int... values) {
    DeltaBinaryPackingValuesWriterForInteger writer =
        new DeltaBinaryPackingValuesWriterForInteger(
            128, 4, 64, 64, HeapByteBufferAllocator.getInstance());
    for (int value : values) {
      writer.writeInteger(value);
    }
    try {
      return writer.getBytes().toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static RawChunk strings(long values, RawPage... pages) {
    return new RawChunk(
        "n", Type.BYTE_ARRAY, CompressionCodec.UNCOMPRESSED, values, List.of(pages));
  }

  @Test
  void deltaEncodedStringsDecodeAndAPrefixLongerThanTheValueBeforeLosesTheRowGroup()
      throws Exception {
    DeltaLengthByteArrayValuesWriter lengths =
        new DeltaLengthByteArrayValuesWriter(64, 64, HeapByteBufferAllocator.getInstance());
    for (String value : List.of("ab", "c", "def")) {
      lengths.writeBytes(Binary.fromString(value));
    }
    byte[] deltaLength = lengths.getBytes().toByteArray();
    byte[] tooLongPrefix = concatAll(deltaInts(0, 5, 0), deltaInts(3, 1, 1), utf8("abcdx"));
    byte[] prefixed = concatAll(deltaInts(0, 2, 1), deltaInts(3, 1, 1), utf8("abcdx"));
    RawChunk valid = strings(3, page(3, Encoding.DELTA_BYTE_ARRAY, prefixed));
    List<Object> expected = new ArrayList<>();
    expected.add(
        new Bad(
            ErrorKind.MALFORMED,
            "",
            1,
            null,
            "MalformedParquetException: parquet DELTA_BYTE_ARRAY value 1 is invalid",
            false));
    expected.add(new Ok(json("{\"n\":\"YWJj\"}"), 4, null));
    expected.add(new Ok(json("{\"n\":\"YWJk\"}"), 5, null));
    expected.add(new Ok(json("{\"n\":\"YXg=\"}"), 6, null));
    assertEquals(
        expected,
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.REQUIRED,
                strings(3, page(3, Encoding.DELTA_BYTE_ARRAY, tooLongPrefix)),
                valid,
                group -> group)));
    assertEquals(
        List.of(
            new Ok(json("{\"n\":\"YWI=\"}"), 1, null),
            new Ok(json("{\"n\":\"Yw==\"}"), 2, null),
            new Ok(json("{\"n\":\"ZGVm\"}"), 3, null),
            new Ok(json("{\"n\":\"YWJj\"}"), 4, null),
            new Ok(json("{\"n\":\"YWJk\"}"), 5, null),
            new Ok(json("{\"n\":\"YXg=\"}"), 6, null)),
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.REQUIRED,
                strings(3, page(3, Encoding.DELTA_LENGTH_BYTE_ARRAY, deltaLength)),
                valid,
                group -> group)));
  }

  @Test
  void codecFailuresLoseTheirRowGroup() throws Exception {
    byte[] snappy = Snappy.compress(ints(1, 2, 3));
    assertEquals(
        lostThenIntact("MalformedParquetException: snappy page holds 12 bytes, not 16", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                new RawChunk(
                    "n",
                    Type.INT32,
                    CompressionCodec.SNAPPY,
                    3,
                    List.of(page(3, Encoding.PLAIN, snappy, 16))),
                group -> group)));
    assertEquals(
        lostThenIntact("MalformedParquetException: parquet codec BROTLI is not supported", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                new RawChunk(
                    "n",
                    Type.INT32,
                    CompressionCodec.BROTLI,
                    3,
                    List.of(page(3, Encoding.PLAIN, ints(1, 2, 3)))),
                group -> group)));
    assertEquals(
        lostThenIntact("MalformedParquetException: parquet data page v2 header is invalid", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(
                    3,
                    new RawPage(
                        new PageHeader(PageType.DATA_PAGE_V2, 12, 12)
                            .setData_page_header_v2(
                                new DataPageHeaderV2(3, 0, 3, Encoding.PLAIN, 10, 10)),
                        ints(1, 2, 3))),
                group -> group)));
  }

  @Test
  void dataThatParquetCannotDecodeLosesTheRestOfItsRowGroup() {
    assertEquals(
        List.of(
            new Ok(json("{\"n\":0}"), 1, null),
            new Ok(json("{\"n\":0}"), 2, null),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                3,
                null,
                "ParquetDecodingException: Can't read value in column [n] required int32 n at value 3"
                    + " out of 3, 3 out of 3 in currentPage. repetition level: 0, definition level: 0",
                false),
            new Ok(json("{\"n\":1}"), 4, null),
            new Ok(json("{\"n\":2}"), 5, null),
            new Ok(json("{\"n\":3}"), 6, null)),
        decode(
            PARQUET,
            hostileThenIntact(column(3, page(3, Encoding.PLAIN, new byte[8])), group -> group)));
  }

  @Test
  void valueThatDoesNotConvertLosesOnlyItsRow() {
    byte[] values = concatAll(ints(1), new byte[] {5}, ints(0), ints(1), new byte[] {7});
    byte[] file =
        ParquetTestFiles.assemble(
            FieldRepetitionType.REQUIRED,
            3,
            List.of(List.of(strings(3, page(3, Encoding.PLAIN, values)))),
            footer -> {
              footer
                  .getSchema()
                  .get(1)
                  .setLogicalType(LogicalType.DECIMAL(new DecimalType(2, 3)))
                  .setScale(2)
                  .setPrecision(3);
              return footer;
            });
    assertEquals(
        List.of(
            new Ok(json("{\"n\":\"0.05\"}"), 1, null),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "MalformedParquetException: column n does not convert: Zero length BigInteger",
                false),
            new Ok(json("{\"n\":\"0.07\"}"), 3, null)),
        decode(PARQUET, file));
  }

  @Test
  void pagesParquetMrDoesNotWriteStillDecode() {
    byte[] raw = LZ4Factory.fastestInstance().fastCompressor().compress(ints(1, 2, 3));
    List<Object> twice =
        List.of(
            new Ok(json("{\"n\":1}"), 1, null),
            new Ok(json("{\"n\":2}"), 2, null),
            new Ok(json("{\"n\":3}"), 3, null),
            new Ok(json("{\"n\":1}"), 4, null),
            new Ok(json("{\"n\":2}"), 5, null),
            new Ok(json("{\"n\":3}"), 6, null));
    assertEquals(
        twice,
        decode(
            PARQUET,
            hostileThenIntact(
                new RawChunk(
                    "n",
                    Type.INT32,
                    CompressionCodec.LZ4_RAW,
                    3,
                    List.of(page(3, Encoding.PLAIN, raw, 12))),
                group -> group)));
    assertEquals(
        twice,
        decode(
            PARQUET,
            hostileThenIntact(
                new RawChunk(
                    "n",
                    Type.INT32,
                    CompressionCodec.SNAPPY,
                    3,
                    List.of(
                        new RawPage(
                            new PageHeader(PageType.DATA_PAGE_V2, 12, 12)
                                .setData_page_header_v2(
                                    new DataPageHeaderV2(3, 0, 3, Encoding.PLAIN, 0, 0)
                                        .setIs_compressed(false)),
                            ints(1, 2, 3)))),
                group -> group)));
  }

  @Test
  void repeatedColumnsMayClaimOnlySoManyListItemsPerByte() {
    byte[] levels = {2, 0, 0, 0, 6, 0, 2, 0, 0, 0, 6, 1};
    RawChunk oneItemEach = column(3, page(3, Encoding.PLAIN, concatAll(levels, ints(1, 2, 3))));
    RawChunk tooManyItems =
        column(2_000_000, page(2_000_000, Encoding.PLAIN, concatAll(levels, ints(1, 2, 3))));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                null,
                "MalformedParquetException: parquet row group claims 1999997 list items in 24 bytes,"
                    + " more than 32 per byte plus 1000000",
                false),
            new Ok(json("{\"n\":[1]}"), 4, null),
            new Ok(json("{\"n\":[2]}"), 5, null),
            new Ok(json("{\"n\":[3]}"), 6, null)),
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.REPEATED, tooManyItems, oneItemEach, group -> group)));
  }

  @Test
  void deltaPagesCountWhatTheirDecoderHoldsAgainstTheDocumentLimit() {
    byte[] header = {(byte) 0x80, 0x01, 0x04, 0x00, 0x02};
    byte[] file =
        hostileThenIntact(
            column(1_000_000, page(1_000_000, Encoding.DELTA_BINARY_PACKED, header)),
            group -> group.setNum_rows(1_000_000));
    long limit = ParquetDecodeTest.footerEstimate(file);
    long estimate = 0;
    while (decodeWithin(file, limit).get(0) instanceof Big big) {
      estimate = big.sizeSoFar();
      limit = estimate;
    }
    assertTrue(estimate > 8 * (1_000_000 + 2048), "estimate: " + estimate);
    assertEquals(
        List.of(
            new Big(1, null, estimate, "", false),
            new Ok(json("{\"n\":1}"), 1_000_001, null),
            new Ok(json("{\"n\":2}"), 1_000_002, null),
            new Ok(json("{\"n\":3}"), 1_000_003, null)),
        decodeWithin(file, estimate - 1));
  }

  @Test
  void bitPackedRunsAreCappedButMayRunPastThePage() {
    byte[] padded = new byte[4 + 1 + 32];
    padded[0] = 33;
    padded[4] = 0x41;
    padded[5] = 7;
    byte[] tooLong = {6, 0, 0, 0, (byte) 0x83, 0x10, 0, 0, 0, 0};
    assertEquals(
        List.of(
            new Ok(json("{\"n\":1}"), 1, null),
            new Ok(json("{\"n\":2}"), 2, null),
            new Ok(json("{\"n\":3}"), 3, null),
            new Ok(json("{\"n\":1}"), 4, null),
            new Ok(json("{\"n\":2}"), 5, null),
            new Ok(json("{\"n\":3}"), 6, null)),
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.OPTIONAL,
                column(3, page(3, Encoding.PLAIN, concatAll(padded, ints(1, 2, 3)))),
                OPTIONAL_VALID,
                group -> group)));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet bit-packed run of 8200 values does not fit the page",
            4),
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.OPTIONAL,
                column(3, page(3, Encoding.PLAIN, concatAll(tooLong, ints(1, 2, 3)))),
                OPTIONAL_VALID,
                group -> group)));
  }

  @Test
  void pageV2WithNoValueBytesDecodesWhateverItsCodec() {
    RawPage allNull =
        new RawPage(
            new PageHeader(PageType.DATA_PAGE_V2, 2, 2)
                .setData_page_header_v2(new DataPageHeaderV2(3, 3, 3, Encoding.PLAIN, 2, 0)),
            new byte[] {6, 0});
    assertEquals(
        List.of(
            new Ok(json("{}"), 1, null),
            new Ok(json("{}"), 2, null),
            new Ok(json("{}"), 3, null),
            new Ok(json("{\"n\":1}"), 4, null),
            new Ok(json("{\"n\":2}"), 5, null),
            new Ok(json("{\"n\":3}"), 6, null)),
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.OPTIONAL,
                new RawChunk("n", Type.INT32, CompressionCodec.SNAPPY, 3, List.of(allNull)),
                OPTIONAL_VALID,
                group -> group)));
  }

  private static byte[] decimalFile(int scale) {
    return ParquetTestFiles.assemble(
        FieldRepetitionType.REQUIRED,
        1,
        List.of(List.of(strings(1, page(1, Encoding.PLAIN, concatAll(ints(1), new byte[] {5}))))),
        footer -> {
          footer
              .getSchema()
              .get(1)
              .setLogicalType(LogicalType.DECIMAL(new DecimalType(scale, scale)))
              .setScale(scale)
              .setPrecision(scale);
          return footer;
        });
  }

  @Test
  void decimalWithAHugeScaleLosesItsRow() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                null,
                "MalformedParquetException: column n does not convert: decimal scale 1001 is over 1000",
                false)),
        decode(PARQUET, decimalFile(1001)));
    assertEquals(
        List.of(new Ok(json("{\"n\":\"0." + "0".repeat(999) + "5\"}"), 1, null)),
        decode(PARQUET, decimalFile(1000)));
    assertEquals(
        List.of(new Big(1, null, 100_000_017, "", false)),
        decode(
            decoder(PARQUET),
            ByteInput.ofBytes(decimalFile(100_000_000)),
            new DecodeLimits(1024 * 1024, 0)));
  }

  @Test
  void deltaBlocksAreCapped() {
    byte[] hugeBlock = {(byte) 0x80, 0x20, 0x04, 0x03, 0x02};
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet delta block of 4096 values in 4 miniblocks", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, page(3, Encoding.DELTA_BINARY_PACKED, hugeBlock)), group -> group)));
  }

  @Test
  void manyEmptyPagesCountAgainstTheDocumentLimit() {
    List<RawPage> pages = new ArrayList<>();
    for (int i = 0; i < 20_000; i++) {
      pages.add(page(0, Encoding.PLAIN, new byte[0]));
    }
    pages.add(page(3, Encoding.PLAIN, ints(1, 2, 3)));
    byte[] file = hostileThenIntact(column(3, pages.toArray(RawPage[]::new)), group -> group);
    List<Object> outcomes = decodeWithin(file, 1024 * 1024);
    long estimate = ((Big) outcomes.get(0)).sizeSoFar();
    assertTrue(
        estimate > 1024 * 1024 && estimate < 1024 * 1024 + 1024, "stops at the limit: " + estimate);
    List<Object> expected = new ArrayList<>(SIX_ROWS.subList(3, 6));
    expected.add(0, new Big(1, null, estimate, "", false));
    assertEquals(expected, outcomes);
  }

  @Test
  void footersThatWouldHoldFarMoreThanTheirBytesAreOversized() {
    byte[] keys =
        ParquetTestFiles.assemble(
            FieldRepetitionType.REQUIRED,
            3,
            List.of(List.of(VALID)),
            footer -> {
              List<KeyValue> values = new ArrayList<>();
              for (int i = 0; i < 100_000; i++) {
                values.add(new KeyValue(""));
              }
              return footer.setKey_value_metadata(values);
            });
    List<SchemaElement> wide = new ArrayList<>();
    wide.add(new SchemaElement("schema").setNum_children(1000));
    for (int i = 0; i < 1000; i++) {
      wide.add(
          new SchemaElement("c" + i)
              .setType(Type.BOOLEAN)
              .setRepetition_type(FieldRepetitionType.OPTIONAL));
    }
    for (byte[] file : List.of(keys, withSchema(wide))) {
      List<Object> outcomes = decodeWithin(file, 1024 * 1024);
      assertEquals(1, outcomes.size());
      Big big = (Big) outcomes.get(0);
      assertTrue(big.sizeSoFar() > 1024 * 1024);
      assertEquals(new Big(1, null, big.sizeSoFar(), "", true), big);
    }
  }

  @Test
  void schemasWhoseRecordReaderTablesWouldBeHugeAreRejected() {
    List<SchemaElement> nested = new ArrayList<>();
    nested.add(new SchemaElement("schema").setNum_children(1));
    for (int i = 0; i < 31; i++) {
      nested.add(
          new SchemaElement("g" + i)
              .setNum_children(i == 30 ? 16 : 1)
              .setRepetition_type(FieldRepetitionType.REPEATED));
    }
    for (int i = 0; i < 16; i++) {
      nested.add(
          new SchemaElement("n" + i)
              .setType(Type.INT32)
              .setRepetition_type(FieldRepetitionType.REPEATED));
    }
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: parquet schema has too many deeply nested columns")),
        decode(PARQUET, withSchema(nested)));
  }

  @Test
  void untrustworthyRowCountsCountAsOneRecord() {
    long cap = 1024 * 29 + (1L << 26);
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet column chunk [n] has 3 values for " + cap + " rows",
            cap + 1),
        decode(PARQUET, hostileThenIntact(VALID, group -> group.setNum_rows(cap))));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet row group claims "
                + (cap + 1)
                + " rows in 29 bytes, more than 1024 per byte plus 67108864",
            2),
        decode(PARQUET, hostileThenIntact(VALID, group -> group.setNum_rows(cap + 1))));
    assertEquals(
        lostThenIntact("MalformedParquetException: parquet row group claims -1 rows", 2),
        decode(PARQUET, hostileThenIntact(VALID, group -> group.setNum_rows(-1))));
  }

  @Test
  void containersMayNotClaimMoreElementsThanBytesLeft() {
    byte[] footer = new byte[25];
    footer[0] = 0x15;
    footer[1] = 0x02;
    footer[2] = 0x19;
    footer[3] = (byte) 0xFC;
    footer[4] = 24;
    assertEquals(
        List.of(
            terminal(
                "MalformedParquetException: parquet FileMetaData does not parse: a container claims 24 elements with 20 bytes left")),
        decode(PARQUET, withRawFooter(footer)));
  }

  @Test
  void estimatesOfSmallFilesArePinned() {
    byte[] file = hostileThenIntact(VALID, group -> group);
    assertEquals(PINNED_FOOTER, ParquetDecodeTest.footerEstimate(file));
    assertEquals(SIX_ROWS, decodeWithin(file, PINNED_FOOTER));
    int[] values = new int[2000];
    Arrays.setAll(values, i -> i);
    byte[] wide =
        ParquetTestFiles.assemble(
            FieldRepetitionType.REQUIRED,
            2000,
            List.of(List.of(column(2000, page(2000, Encoding.PLAIN, ints(values))))),
            footer -> footer);
    assertEquals(
        List.of(3402L, 8020L, 16344L),
        List.of(
            ParquetDecodeTest.footerEstimate(wide),
            ((Big) decodeWithin(wide, 3402).get(0)).sizeSoFar(),
            ((Big) decodeWithin(wide, 8020).get(0)).sizeSoFar()));
    List<Object> rows = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      rows.add(new Ok(json("{\"n\":" + i + "}"), i + 1, null));
    }
    assertEquals(rows, decodeWithin(wide, 16344));
  }

  @Test
  void bitPackedRunsOfExactlyTheCapDecode() {
    byte[] levels = new byte[4 + 2 + 1024];
    levels[0] = 2;
    levels[1] = 4;
    levels[4] = (byte) 0x81;
    levels[5] = 0x10;
    levels[6] = 7;
    assertEquals(
        SIX_ROWS,
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.OPTIONAL,
                column(3, page(3, Encoding.PLAIN, concatAll(levels, ints(1, 2, 3)))),
                OPTIONAL_VALID,
                group -> group)));
  }

  @Test
  void deltaBlocksOfExactlyTheCapDecode() {
    byte[] atCap = {(byte) 0x80, 0x10, 0x04, 0x03, 0x02, 0x02, 0, 0, 0, 0};
    byte[] overCap = {(byte) 0xA0, 0x10, 0x04, 0x03, 0x02, 0x02, 0, 0, 0, 0};
    assertEquals(
        SIX_ROWS,
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, page(3, Encoding.DELTA_BINARY_PACKED, atCap)), group -> group)));
    assertEquals(
        lostThenIntact(
            "MalformedParquetException: parquet delta block of 2080 values in 4 miniblocks", 4),
        decode(
            PARQUET,
            hostileThenIntact(
                column(3, page(3, Encoding.DELTA_BINARY_PACKED, overCap)), group -> group)));
  }

  @Test
  void denseDeltaPagesDecode() throws IOException {
    DeltaBinaryPackingValuesWriterForInteger writer =
        new DeltaBinaryPackingValuesWriterForInteger(
            2048, 4, 64, 64, HeapByteBufferAllocator.getInstance());
    int rows = 100_000;
    for (int i = 0; i < rows; i++) {
      writer.writeInteger(i);
    }
    byte[] values = writer.getBytes().toByteArray();
    assertTrue(rows / values.length > 100, "values per byte: " + rows / values.length);
    byte[] file =
        ParquetTestFiles.assemble(
            FieldRepetitionType.REQUIRED,
            rows,
            List.of(List.of(column(rows, page(rows, Encoding.DELTA_BINARY_PACKED, values)))),
            footer -> footer);
    List<Object> expected = new ArrayList<>();
    for (int i = 0; i < rows; i++) {
      expected.add(new Ok(json("{\"n\":" + i + "}"), i + 1, null));
    }
    assertEquals(expected, decode(PARQUET, file));
  }

  @Test
  void binaryDictionaryEntriesMustLieInsideThePage() {
    byte[] entries = concatAll(ints(1000), utf8("abc"));
    RawPage dictionary =
        new RawPage(
            new PageHeader(PageType.DICTIONARY_PAGE, entries.length, entries.length)
                .setDictionary_page_header(new DictionaryPageHeader(1, Encoding.PLAIN)),
            entries);
    List<Object> expected = new ArrayList<>();
    expected.add(
        new Bad(
            ErrorKind.MALFORMED,
            "",
            1,
            null,
            "MalformedParquetException: parquet dictionary entry 0 of 1000 bytes overflows the page",
            false));
    expected.add(new Ok(json("{\"n\":\"YWJj\"}"), 4, null));
    expected.add(new Ok(json("{\"n\":\"YWJk\"}"), 5, null));
    expected.add(new Ok(json("{\"n\":\"YXg=\"}"), 6, null));
    byte[] prefixed = concatAll(deltaInts(0, 2, 1), deltaInts(3, 1, 1), utf8("abcdx"));
    assertEquals(
        expected,
        decode(
            PARQUET,
            hostileThenIntact(
                FieldRepetitionType.REQUIRED,
                strings(3, dictionary, page(3, Encoding.RLE_DICTIONARY, new byte[] {0, 6})),
                strings(3, page(3, Encoding.DELTA_BYTE_ARRAY, prefixed)),
                group -> group)));
  }

  @Test
  void schemaCellsAtAndOverTheCap() {
    for (int leaves : List.of(413, 414)) {
      List<SchemaElement> nested = new ArrayList<>();
      nested.add(new SchemaElement("schema").setNum_children(1));
      for (int i = 0; i < 9; i++) {
        nested.add(
            new SchemaElement("g" + i)
                .setNum_children(i == 8 ? leaves : 1)
                .setRepetition_type(FieldRepetitionType.REPEATED));
      }
      for (int i = 0; i < leaves; i++) {
        nested.add(
            new SchemaElement("n" + i)
                .setType(Type.INT32)
                .setRepetition_type(FieldRepetitionType.REPEATED));
      }
      List<Object> outcomes = decode(PARQUET, withSchema(nested));
      assertEquals(
          leaves == 414
              ? List.of(
                  terminal(
                      "MalformedParquetException: parquet schema has too many deeply nested columns"))
              : List.of(
                  new Bad(
                      ErrorKind.MALFORMED,
                      "",
                      1,
                      null,
                      "MalformedParquetException: parquet row group has 1 column chunks for 413 columns",
                      false)),
          outcomes);
    }
  }
}
