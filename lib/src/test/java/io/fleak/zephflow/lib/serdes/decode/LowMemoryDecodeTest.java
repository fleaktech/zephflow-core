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

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.concat;
import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.decoder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.tukaani.xz.LZMA2InputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

/**
 * Runs in the lowMemoryTest task with a 64 MiB heap. Every input here is several times larger than
 * the heap and is generated on the fly, so a decoder that buffers the whole input fails with OOM.
 */
@Tag("lowMemory")
class LowMemoryDecodeTest {

  private static final long TARGET_BYTES = 300L * 1024 * 1024;
  private static final DecodeLimits LIMITS = new DecodeLimits(1024 * 1024, 4 * 1024 * 1024);
  private static final String PAD = "x".repeat(200);
  private static final String XML_EVENTS =
      "{\"format\":{\"type\":\"xml\",\"mode\":\"elements\",\"recordPath\":\"/Events/Event\"}}";
  private static final String YAML_MULTI =
      "{\"format\":{\"type\":\"yaml\",\"mode\":\"multi_document\"}}";
  private static final String MULTILINE =
      "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"continuePattern\":\"^\\\\s\"}}}";

  @BeforeAll
  static void heapIsSmallerThanTheInputs() {
    assertTrue(
        Runtime.getRuntime().maxMemory() < TARGET_BYTES / 2,
        "run through the lowMemoryTest task so the heap is bounded");
  }

  @Test
  void jsonLinesFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input =
        generated(records, i -> "{\"i\":" + i + ",\"p\":\"" + PAD + "\"}\n", "", "");
    assertEquals(
        Map.of("Decoded", records),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"), input, LIMITS));
  }

  @Test
  void jsonArrayFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input =
        generated(
            records,
            i -> (i == 0 ? "" : ",") + "{\"i\":" + i + ",\"p\":\"" + PAD + "\"}",
            "[",
            "]");
    assertEquals(
        Map.of("Decoded", records),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"array\"}}"), input, LIMITS));
  }

  @Test
  void gzippedTextLinesFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 210;
    InputStream input = gzipped(generated(records, i -> PAD + i + "\n", "", ""));
    assertEquals(
        Map.of("Decoded", records),
        count(
            decoder(
                "{\"compression\":\"gzip\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}"),
            input,
            LIMITS));
  }

  @Test
  void hugeSingleLineIsOversizedAndTheNextLineStillDecodes() {
    InputStream input = generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "", "\n{\"ok\":1}\n");
    assertEquals(
        Map.of("Oversized", 1L, "Decoded", 1L),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"), input, LIMITS));
  }

  @Test
  void hugeArrayElementIsOversized() {
    InputStream input =
        generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "[{\"ok\":1},{\"s\":\"", "\"}]");
    assertEquals(
        Map.of("Decoded", 1L, "Oversized", 1L),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"array\"}}"), input, LIMITS));
  }

  @Test
  void hugeDocumentIsOversized() {
    InputStream input = generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "{\"s\":\"", "\"}");
    assertEquals(
        Map.of("Oversized", 1L),
        count(decoder("{\"format\":{\"type\":\"json\",\"mode\":\"object\"}}"), input, LIMITS));
  }

  @Test
  void multilineEventsFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input = generated(records, i -> "E" + i + "\n\tat " + PAD + "\n\tat x\n", "", "");
    assertEquals(Map.of("Decoded", records), count(decoder(MULTILINE), input, LIMITS));
  }

  @Test
  void hugeMultilineEventIsOversizedAndTheNextEventStillDecodes() {
    InputStream input =
        generated(TARGET_BYTES / 1000, i -> "\t" + "x".repeat(999) + "\n", "start\n", "next\n");
    assertEquals(Map.of("Oversized", 1L, "Decoded", 1L), count(decoder(MULTILINE), input, LIMITS));
  }

  @Test
  void utf16TextLinesFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 420;
    InputStream input = generated(records, i -> PAD + i + "\n", "", "", StandardCharsets.UTF_16LE);
    assertEquals(
        Map.of("Decoded", records),
        count(
            decoder(
                "{\"format\":{\"type\":\"text\",\"charset\":\"UTF-16LE\",\"framing\":{\"type\":\"line\"}}}"),
            input,
            LIMITS));
  }

  @Test
  void csvFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input = generated(records, i -> i + ",\"" + PAD + "\"\n", "id,pad\n", "");
    assertEquals(
        Map.of("Decoded", records),
        count(decoder("{\"format\":{\"type\":\"csv\"}}"), input, LIMITS));
  }

  @Test
  void hugeCsvFieldIsOversizedAndTheNextRowStillDecodes() {
    InputStream input =
        generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "id,pad\n1,\"", "\"\n2,ok\n");
    assertEquals(
        Map.of("Oversized", 1L, "Decoded", 1L),
        count(decoder("{\"format\":{\"type\":\"csv\"}}"), input, LIMITS));
  }

  @Test
  void xmlElementsFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 230;
    InputStream input =
        generated(
            records,
            i -> "<Event id=\"" + i + "\"><p>" + PAD + "</p></Event>",
            "<Events>",
            "</Events>");
    assertEquals(Map.of("Decoded", records), count(decoder(XML_EVENTS), input, LIMITS));
  }

  @Test
  void hugeXmlElementIsOversized() {
    InputStream input =
        generated(
            TARGET_BYTES / 1000,
            i -> "x".repeat(1000),
            "<Events><Event>1</Event><Event>",
            "</Event></Events>");
    assertEquals(Map.of("Decoded", 1L, "Oversized", 1L), count(decoder(XML_EVENTS), input, LIMITS));
  }

  @Test
  void yamlDocumentsFarLargerThanTheHeap() {
    long records = TARGET_BYTES / 220;
    InputStream input = generated(records, i -> "---\ni: " + i + "\np: " + PAD + "\n", "", "");
    assertEquals(Map.of("Decoded", records), count(decoder(YAML_MULTI), input, LIMITS));
  }

  @Test
  void hugeYamlDocumentIsOversized() {
    InputStream input =
        generated(TARGET_BYTES / 1000, i -> "x".repeat(1000), "a: 1\n---\nb: ", "\n");
    assertEquals(Map.of("Decoded", 1L, "Oversized", 1L), count(decoder(YAML_MULTI), input, LIMITS));
  }

  @Test
  void otherCodecsDecodeInputsFarLargerThanTheHeap() {
    record Case(String codec, Compressor compressor, long bytes) {}
    List<Case> cases =
        List.of(
            new Case("zstd", ZstdOutputStreamNoFinalizer::new, TARGET_BYTES),
            new Case("lz4", FramedLZ4CompressorOutputStream::new, TARGET_BYTES),
            new Case("snappy", FramedSnappyCompressorOutputStream::new, TARGET_BYTES),
            new Case("deflate", DeflaterOutputStream::new, TARGET_BYTES),
            new Case("xz", out -> new XZOutputStream(out, new LZMA2Options(1)), TARGET_BYTES),
            // bzip2 compresses slowly; 100 MB is still well above the 64 MiB heap.
            new Case("bzip2", BZip2CompressorOutputStream::new, TARGET_BYTES / 3));
    for (Case c : cases) {
      long records = c.bytes() / 210;
      InputStream input =
          compressed(generated(records, i -> PAD + i + "\n", "", ""), c.compressor());
      assertEquals(
          Map.of("Decoded", records),
          count(
              decoder(
                  "{\"compression\":\""
                      + c.codec()
                      + "\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}"),
              input,
              LIMITS),
          c.codec());
    }
  }

  @Test
  void xzWithoutADocumentLimitIsCappedAtHalfTheHeap() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (OutputStream out = new XZOutputStream(bytes, new LZMA2Options(0))) {
      out.write("a\n".getBytes(StandardCharsets.UTF_8));
    }
    byte[] xz = bytes.toByteArray();
    // Declare a 48 MiB dictionary (LZMA2 property 27) and fix the block header's CRC32.
    int headerSize = ((xz[12] & 0xFF) + 1) * 4;
    xz[16] = 27;
    CRC32 crc = new CRC32();
    crc.update(xz, 12, headerSize - 4);
    for (int i = 0; i < 4; i++) {
      xz[12 + headerSize - 4 + i] = (byte) (crc.getValue() >>> (8 * i));
    }
    long needed = LZMA2InputStream.getMemoryUsage(48 << 20) * 1024L;
    assertTrue(
        needed > Runtime.getRuntime().maxMemory() / 2, "the dictionary must exceed half the heap");
    assertEquals(
        Map.of("Oversized", 1L),
        count(
            decoder(
                "{\"compression\":\"xz\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}"),
            new ByteArrayInputStream(xz),
            new DecodeLimits(1024 * 1024, 0)));
  }

  private static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"i\",\"type\":\"long\"},"
                  + "{\"name\":\"p\",\"type\":\"string\"}]}");

  /** The header of an avro file with no blocks; its last 16 bytes are the sync marker. */
  private static byte[] avroHeader() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(AVRO_SCHEMA))) {
      writer.create(AVRO_SCHEMA, bytes);
    }
    return bytes.toByteArray();
  }

  private static byte[] avroRecords(long firstRecord, int records) throws IOException {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(data, null);
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(AVRO_SCHEMA);
    for (int r = 0; r < records; r++) {
      GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
      record.put("i", firstRecord + r);
      record.put("p", PAD);
      writer.write(record, encoder);
    }
    encoder.flush();
    return data.toByteArray();
  }

  private static byte[] avroBlock(byte[] sync, long firstRecord, int records) throws IOException {
    return avroRawBlock(sync, records, avroRecords(firstRecord, records));
  }

  @Test
  void avroFileFarLargerThanTheHeap() throws IOException {
    byte[] header = avroHeader();
    byte[] sync = Arrays.copyOfRange(header, header.length - 16, header.length);
    long blocks = TARGET_BYTES / (210L * 1000);
    Enumeration<InputStream> parts =
        new Enumeration<>() {
          private long next = -1;

          @Override
          public boolean hasMoreElements() {
            return next < blocks;
          }

          @Override
          public InputStream nextElement() {
            try {
              byte[] part = next < 0 ? header : avroBlock(sync, next * 1000, 1000);
              next++;
              return new ByteArrayInputStream(part);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }
        };
    assertEquals(
        Map.of("Decoded", blocks * 1000),
        count(
            decoder("{\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}"),
            new SequenceInputStream(parts),
            LIMITS));
  }

  @Test
  void hugeAvroBlockIsSkippedAndTheNextBlockDecodes() throws IOException {
    byte[] header = avroHeader();
    byte[] sync = Arrays.copyOfRange(header, header.length - 16, header.length);
    ByteArrayOutputStream hugeHead = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(hugeHead, null);
    long hugeSize = (TARGET_BYTES / 1000) * 1000;
    encoder.writeLong(1);
    encoder.writeLong(hugeSize);
    encoder.flush();
    InputStream input =
        new SequenceInputStream(
            Collections.enumeration(
                List.of(
                    new ByteArrayInputStream(concat(header, hugeHead.toByteArray())),
                    generated(hugeSize / 1000, i -> "x".repeat(1000), "", ""),
                    new ByteArrayInputStream(concat(sync, avroBlock(sync, 0, 1))))));
    assertEquals(
        Map.of("Oversized", 1L, "Decoded", 1L),
        count(decoder("{\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}"), input, LIMITS));
  }

  /** Compresses {@code size} bytes of {@code data} the way avro's codec of that name does. */
  private static byte[] avroCompress(String codec, InputStream data, long size) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    if (codec.equals("snappy")) {
      CheckedInputStream checked = new CheckedInputStream(data, new CRC32());
      try (OutputStream out = new SnappyCompressorOutputStream(bytes, size)) {
        checked.transferTo(out);
      }
      bytes.write(ByteBuffer.allocate(4).putInt((int) checked.getChecksum().getValue()).array());
      return bytes.toByteArray();
    }
    try (OutputStream out =
        switch (codec) {
          case "deflate" -> new DeflaterOutputStream(bytes, new Deflater(1, true));
          case "zstandard" -> new ZstdOutputStreamNoFinalizer(bytes);
          case "bzip2" -> new BZip2CompressorOutputStream(bytes);
          case "xz" -> new XZOutputStream(bytes, new LZMA2Options(1));
          default -> throw new IllegalArgumentException(codec);
        }) {
      data.transferTo(out);
    }
    return bytes.toByteArray();
  }

  private static byte[] avroRawBlock(byte[] sync, long count, byte[] data) throws IOException {
    ByteArrayOutputStream block = new ByteArrayOutputStream();
    BinaryEncoder header = EncoderFactory.get().binaryEncoder(block, null);
    header.writeLong(count);
    header.writeLong(data.length);
    header.flush();
    block.writeBytes(data);
    block.writeBytes(sync);
    return block.toByteArray();
  }

  @Test
  void avroDecompressionBombsAreOversizedForEveryCodec() throws IOException {
    for (String codec : List.of("deflate", "snappy", "zstandard", "bzip2", "xz")) {
      ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
      try (DataFileWriter<GenericRecord> writer =
          new DataFileWriter<>(new GenericDatumWriter<>(AVRO_SCHEMA))) {
        writer.setCodec(CodecFactory.fromString(codec));
        writer.create(AVRO_SCHEMA, headerBytes);
      }
      byte[] header = headerBytes.toByteArray();
      byte[] sync = Arrays.copyOfRange(header, header.length - 16, header.length);
      // One record whose string field is 100 MB of 'x': the long 0, then the string's length.
      ByteArrayOutputStream prefix = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(prefix, null);
      long length = 100L * 1024 * 1024;
      encoder.writeLong(0);
      encoder.writeLong(length);
      encoder.flush();
      InputStream bomb =
          new SequenceInputStream(
              new ByteArrayInputStream(prefix.toByteArray()),
              generated(length / 1024, i -> "x".repeat(1024), "", ""));
      byte[] small = avroRecords(0, 1);
      InputStream input =
          new ByteArrayInputStream(
              concat(
                  header,
                  avroRawBlock(sync, 1, avroCompress(codec, bomb, prefix.size() + length)),
                  avroRawBlock(
                      sync,
                      1,
                      avroCompress(codec, new ByteArrayInputStream(small), small.length))));
      assertEquals(
          Map.of("Oversized", 1L, "Decoded", 1L),
          count(decoder("{\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}"), input, LIMITS),
          codec);
    }
  }

  private static Map<String, Long> count(
      RecordDecoder decoder, InputStream input, DecodeLimits limits) {
    Map<String, Long> counts = new LinkedHashMap<>();
    try (RecordCursor cursor = decoder.open(() -> input, limits)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        counts.merge(outcome.getClass().getSimpleName(), 1L, Long::sum);
      }
    } catch (SourceReadException e) {
      throw new UncheckedIOException(e);
    }
    return counts;
  }

  private static InputStream generated(
      long count, LongFunction<String> item, String prefix, String suffix) {
    return generated(count, item, prefix, suffix, StandardCharsets.UTF_8);
  }

  private static InputStream generated(
      long count, LongFunction<String> item, String prefix, String suffix, Charset charset) {
    return new InputStream() {
      private long next;
      private byte[] current = prefix.getBytes(charset);
      private int pos;
      private boolean suffixDone;

      @Override
      public int read() {
        byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
      }

      @Override
      public int read(byte[] b, int off, int len) {
        while (pos == current.length) {
          if (next < count) {
            current = item.apply(next++).getBytes(charset);
          } else if (!suffixDone) {
            suffixDone = true;
            current = suffix.getBytes(charset);
          } else {
            return -1;
          }
          pos = 0;
        }
        int n = Math.min(len, current.length - pos);
        System.arraycopy(current, pos, b, off, n);
        pos += n;
        return n;
      }
    };
  }

  @FunctionalInterface
  interface Compressor {
    OutputStream wrap(OutputStream out) throws IOException;
  }

  private static InputStream gzipped(InputStream plain) {
    return compressed(plain, out -> new GZIPOutputStream(out, 64 * 1024));
  }

  private static InputStream compressed(InputStream plain, Compressor compressor) {
    try {
      PipedInputStream in = new PipedInputStream(64 * 1024);
      OutputStream out = new PipedOutputStream(in);
      Thread writer =
          new Thread(
              () -> {
                try (OutputStream compressing = compressor.wrap(out)) {
                  plain.transferTo(compressing);
                } catch (IOException ignored) {
                  // The reader closed early.
                }
              });
      writer.setDaemon(true);
      writer.start();
      return in;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
