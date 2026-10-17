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

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.decoder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Parquet under the lowMemoryTest task's 64 MiB heap. */
@Tag("lowMemory")
class ParquetLowMemoryTest {

  private static final long TARGET_BYTES = 300L * 1024 * 1024;
  private static final DecodeLimits LIMITS = new DecodeLimits(1024 * 1024, 4 * 1024 * 1024);
  private static final String PARQUET = "{\"format\":{\"type\":\"parquet\"}}";

  @TempDir Path dir;

  @BeforeAll
  static void heapIsSmallerThanTheInputs() {
    assertTrue(
        Runtime.getRuntime().maxMemory() < TARGET_BYTES / 2,
        "run through the lowMemoryTest task so the heap is bounded");
  }

  private record FileRanges(Path file) implements RandomAccessInput {
    @Override
    public long length() throws IOException {
      return Files.size(file);
    }

    @Override
    public InputStream read(long offset, long length) throws IOException {
      return Channels.newInputStream(FileChannel.open(file).position(offset));
    }
  }

  private static ByteInput ranges(Path file) {
    return new ByteInput() {
      @Override
      public InputStream openStream() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Optional<RandomAccessInput> randomAccess() {
        return Optional.of(new FileRanges(file));
      }
    };
  }

  private static Map<String, Long> count(ByteInput input, DecodeLimits limits) {
    Map<String, Long> counts = new LinkedHashMap<>();
    try (RecordCursor cursor = decoder(PARQUET).open(input, limits)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        counts.merge(outcome.getClass().getSimpleName(), 1L, Long::sum);
      }
    } catch (SourceReadException e) {
      throw new UncheckedIOException(e);
    }
    return counts;
  }

  @Test
  void parquetFileFarLargerThanTheHeap() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message m { required int64 n; required binary s (STRING); }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    String pad = "x".repeat(1000);
    long rows = TARGET_BYTES / 1000;
    Iterable<Group> generated =
        () ->
            new Iterator<>() {
              private long next;

              @Override
              public boolean hasNext() {
                return next < rows;
              }

              @Override
              public Group next() {
                long n = next++;
                return factory.newGroup().append("n", n).append("s", pad + n);
              }
            };
    Path file = dir.resolve("big.parquet");
    ParquetTestFiles.write(
        file,
        schema,
        generated,
        options ->
            options
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withPageSize(256 * 1024)
                .withRowGroupSize(1536 * 1024));
    assertTrue(file.toFile().length() > TARGET_BYTES);
    assertEquals(Map.of("Decoded", rows), count(ranges(file), LIMITS));
  }

  @Test
  void hugeRowGroupIsSkippedUnreadAndTheNextOneDecodes() throws IOException {
    byte[] small =
        ParquetTestFiles.assemble(
            FieldRepetitionType.REQUIRED, 3, List.of(List.of(ParquetHostileTest.VALID)), f -> f);
    FileMetaData footer = ParquetDecodeTest.footer(small);
    int footerLength =
        ByteBuffer.wrap(small, small.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    byte[] data = Arrays.copyOf(small, small.length - 8 - footerLength);
    long huge = 200L * 1024 * 1024;
    RowGroup intact = footer.getRow_groups().get(0);
    RowGroup hugeGroup = intact.deepCopy();
    hugeGroup.getColumns().get(0).setFile_offset(data.length);
    hugeGroup
        .getColumns()
        .get(0)
        .getMeta_data()
        .setData_page_offset(data.length)
        .setTotal_compressed_size(huge);
    footer.setRow_groups(List.of(hugeGroup, intact));
    byte[] tail = ParquetTestFiles.withFooter(new byte[0], ParquetTestFiles.thrift(footer));
    Path file = dir.resolve("huge-row-group.parquet");
    try (RandomAccessFile out = new RandomAccessFile(file.toFile(), "rw")) {
      out.write(data);
      out.setLength(data.length + huge);
      out.seek(data.length + huge);
      out.write(tail);
    }
    assertEquals(Map.of("Oversized", 1L, "Decoded", 3L), count(ranges(file), LIMITS));
  }

  /**
   * Each claim would make parquet allocate tens of megabytes that the heap could just hold: a
   * 10,000,000-entry dictionary, a bit-packed run of 12,000,000 levels, a 10,000,000-value delta
   * block, and a 60 MiB page.
   */
  @Test
  void countsThatWouldAllocateTensOfMegabytesAreRejectedFirst() {
    PageHeader dictionary =
        new PageHeader(PageType.DICTIONARY_PAGE, 4, 4)
            .setDictionary_page_header(new DictionaryPageHeader(10_000_000, Encoding.PLAIN));
    assertEquals(
        ParquetHostileTest.lostThenIntact(
            "MalformedParquetException: parquet dictionary of 10000000 entries does not fit in 4 bytes",
            4),
        DecodeTestSupport.decode(
            PARQUET,
            ParquetHostileTest.hostileThenIntact(
                ParquetHostileTest.column(
                    3,
                    new ParquetTestFiles.RawPage(dictionary, ParquetHostileTest.ints(9)),
                    ParquetHostileTest.page(3, Encoding.RLE_DICTIONARY, new byte[] {1, 6, 0})),
                group -> group)));
    byte[] levels = {6, 0, 0, 0, (byte) 0xC1, (byte) 0x8D, (byte) 0xB7, (byte) 0x01, 0, 0};
    assertEquals(
        ParquetHostileTest.lostThenIntact(
            "MalformedParquetException: parquet bit-packed run of 12000000 values does not fit the page",
            4),
        DecodeTestSupport.decode(
            PARQUET,
            ParquetHostileTest.hostileThenIntact(
                FieldRepetitionType.OPTIONAL,
                ParquetHostileTest.column(
                    3,
                    ParquetHostileTest.page(
                        3,
                        Encoding.PLAIN,
                        ParquetHostileTest.concatAll(levels, ParquetHostileTest.ints(1, 2, 3)))),
                ParquetHostileTest.OPTIONAL_VALID,
                group -> group)));
    byte[] delta = {(byte) 0x80, 0x01, 0x04, (byte) 0x80, (byte) 0xAD, (byte) 0xE2, 0x04, 0x02};
    assertEquals(
        ParquetHostileTest.lostThenIntact(
            "MalformedParquetException: parquet delta encoding claims 10000000 values in a page of 3",
            4),
        DecodeTestSupport.decode(
            PARQUET,
            ParquetHostileTest.hostileThenIntact(
                ParquetHostileTest.column(
                    3, ParquetHostileTest.page(3, Encoding.DELTA_BINARY_PACKED, delta)),
                group -> group)));
    byte[] page =
        ParquetHostileTest.hostileThenIntact(
            ParquetHostileTest.column(
                3,
                ParquetHostileTest.page(
                    3, Encoding.PLAIN, ParquetHostileTest.ints(1, 2, 3), 60 * 1024 * 1024)),
            group -> group);
    List<Object> pageOutcomes =
        DecodeTestSupport.decode(decoder(PARQUET), ByteInput.ofBytes(page), LIMITS);
    long pageEstimate = ((DecodeTestSupport.Big) pageOutcomes.get(0)).sizeSoFar();
    assertTrue(pageEstimate > 60 * 1024 * 1024);
    List<Object> expected =
        new ArrayList<>(ParquetHostileTest.lostThenIntact("unused", 4).subList(1, 4));
    expected.add(0, new DecodeTestSupport.Big(1, null, pageEstimate, "", false));
    assertEquals(expected, pageOutcomes);
  }

  @Test
  void rowGroupsNearTheDocumentLimitWithDictionariesDecode() {
    MessageType schema =
        MessageTypeParser.parseMessageType(
            "message m { required int64 n; required binary s (STRING); optional binary t (STRING); }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    String pad = "y".repeat(100);
    long rows = 400_000;
    Iterable<Group> generated =
        () ->
            new Iterator<>() {
              private long next;

              @Override
              public boolean hasNext() {
                return next < rows;
              }

              @Override
              public Group next() {
                long n = next++;
                Group row = factory.newGroup().append("n", n).append("s", pad + (n % 5000));
                return n % 3 == 0 ? row : row.append("t", "t" + (n % 50));
              }
            };
    Path file = dir.resolve("dictionaries.parquet");
    ParquetTestFiles.write(
        file,
        schema,
        generated,
        options ->
            options
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withDictionaryEncoding(true)
                .withDictionaryPageSize(2 * 1024 * 1024)
                .withRowGroupSize(1024 * 1024));
    assertEquals(Map.of("Decoded", rows), count(ranges(file), LIMITS));
  }

  @Test
  void aMillionAllNullRowsInOnePageDecode() {
    MessageType schema = MessageTypeParser.parseMessageType("message m { optional int32 x; }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    long rows = 1 << 20;
    Iterable<Group> generated =
        () ->
            new Iterator<>() {
              private long next;

              @Override
              public boolean hasNext() {
                return next < rows;
              }

              @Override
              public Group next() {
                next++;
                return factory.newGroup();
              }
            };
    Path file = dir.resolve("all-null.parquet");
    ParquetTestFiles.write(
        file, schema, generated, options -> options.withPageRowCountLimit(2_000_000));
    assertTrue(file.toFile().length() < 1024);
    assertEquals(Map.of("Decoded", rows), count(ranges(file), LIMITS));
  }

  @Test
  void manyEmptyPagesStopAtTheLimitBeforeTheirHeadersFillTheHeap() throws IOException {
    byte[] template =
        ParquetHostileTest.hostileThenIntact(ParquetHostileTest.VALID, group -> group);
    FileMetaData footer = ParquetDecodeTest.footer(template);
    int footerLength =
        ByteBuffer.wrap(template, template.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    byte[] empty =
        ParquetTestFiles.thrift(ParquetHostileTest.page(0, Encoding.PLAIN, new byte[0]).header());
    int pages = 240_000;
    long inserted = (long) empty.length * pages;
    ColumnMetaData hostile = footer.getRow_groups().get(0).getColumns().get(0).getMeta_data();
    hostile.setTotal_compressed_size(hostile.getTotal_compressed_size() + inserted);
    ColumnChunk intact = footer.getRow_groups().get(1).getColumns().get(0);
    intact.setFile_offset(intact.getFile_offset() + inserted);
    intact
        .getMeta_data()
        .setData_page_offset(intact.getMeta_data().getData_page_offset() + inserted);
    Path file = dir.resolve("empty-pages.parquet");
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
      out.write(template, 0, 4);
      for (int i = 0; i < pages; i++) {
        out.write(empty);
      }
      out.write(template, 4, template.length - 12 - footerLength);
      out.write(ParquetTestFiles.withFooter(new byte[0], ParquetTestFiles.thrift(footer)));
    }
    assertTrue(Files.size(file) > 4_000_000);
    for (DecodeLimits limits : List.of(LIMITS, DecodeLimits.UNLIMITED)) {
      assertEquals(
          Map.of("Oversized", 1L, "Decoded", 3L), count(ranges(file), limits), limits.toString());
    }
  }
}
