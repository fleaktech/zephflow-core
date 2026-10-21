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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.ByteInput;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MessageDecodingConverterTest {

  private static final String RESEND =
      "the DLQ value is the message as received, so it can be sent again";
  private static final DecodingConfig JSON_OBJECT =
      new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT));
  private static final DecodingConfig JSON_LINES =
      new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES));

  private FleakCounter dataSize;
  private FleakCounter inputCount;
  private FleakCounter failures;
  private SourceExecutionContext<SerializedEvent> context;

  @BeforeEach
  void setUp() {
    dataSize = mock();
    inputCount = mock();
    failures = mock();
    context = new SourceExecutionContext<>(null, null, null, dataSize, inputCount, failures, null);
  }

  @Test
  void decodesOneRecord() {
    SerializedEvent message = message("{\"a\":1,\"__tag__\":{\"t\":\"x\"}}");

    ConvertedResult<SerializedEvent> result = convert(JSON_OBJECT, message);

    assertEquals(
        ConvertedResult.success(
            List.of(record(Map.of("a", 1, "__tag__", Map.of("t", "x")))), message),
        result);
    verify(dataSize).increase(message.value().length, Map.of("t", "x"));
    verify(inputCount).increase(1, Map.of("t", "x"));
    verifyNoInteractions(failures);
  }

  @Test
  void decodesSeveralRecordsInOrder() {
    SerializedEvent message = message("{\"a\":1}\n{\"a\":2}\n");

    ConvertedResult<SerializedEvent> result = convert(JSON_LINES, message);

    assertEquals(
        ConvertedResult.success(List.of(record(Map.of("a", 1)), record(Map.of("a", 2))), message),
        result);
    verify(inputCount).increase(2, Map.of());
  }

  @Test
  void emptyValueGivesNoRecords() {
    SerializedEvent message = new SerializedEvent(null, new byte[0], null);

    assertEquals(ConvertedResult.success(List.of(), message), convert(JSON_OBJECT, message));
    verify(dataSize).increase(0, Map.of());
    verify(inputCount).increase(0, Map.of());
    verifyNoInteractions(failures);
  }

  @Test
  void nullValueGivesNoRecordsWithoutDecoding() {
    SerializedEvent tombstone =
        new SerializedEvent("k".getBytes(StandardCharsets.UTF_8), null, null);
    RecordDecoder decoder = mock();

    ConvertedResult<SerializedEvent> result =
        new MessageDecodingConverter(
                decoder, JSON_OBJECT.format(), MessageDecodingConverter.Unit.MESSAGE)
            .convert(tombstone, context);

    assertEquals(ConvertedResult.success(List.of(), tombstone), result);
    verifyNoInteractions(decoder);
    verify(dataSize).increase(0, Map.of());
    verify(inputCount).increase(0, Map.of());
  }

  @Test
  void metadataAndKeyOverwriteRecordFields() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("a", "from metadata");
    metadata.put("kinesis_hash_key", null);
    SerializedEvent message =
        new SerializedEvent(
            "key".getBytes(StandardCharsets.UTF_8),
            "{\"a\":1,\"b\":2}\n{\"b\":3}".getBytes(StandardCharsets.UTF_8),
            metadata);

    ConvertedResult<SerializedEvent> result = convert(JSON_LINES, message);

    Map<String, FleakData> first = new HashMap<>();
    first.put("a", new StringPrimitiveFleakData("from metadata"));
    first.put("b", FleakData.wrap(2));
    first.put("kinesis_hash_key", new StringPrimitiveFleakData(null));
    first.put("__md_key", new StringPrimitiveFleakData("a2V5"));
    Map<String, FleakData> second = new HashMap<>(first);
    second.put("b", FleakData.wrap(3));
    assertEquals(
        ConvertedResult.success(
            List.of(new RecordFleakData(first), new RecordFleakData(second)), message),
        result);
  }

  @Test
  void keyWithoutMetadataIsNotMerged() {
    SerializedEvent message =
        new SerializedEvent(
            "key".getBytes(StandardCharsets.UTF_8),
            "{\"a\":1}".getBytes(StandardCharsets.UTF_8),
            null);

    assertEquals(
        ConvertedResult.success(List.of(record(Map.of("a", 1))), message),
        convert(JSON_OBJECT, message));
  }

  @Test
  void malformedRecordFailsTheWholeMessage() {
    SerializedEvent message = message("{\"a\":1}\nnot json\n{\"a\":3}\n");

    ConvertedResult<SerializedEvent> result;
    List<String> errors;
    try (BoundedLogCapture logs = new BoundedLogCapture(MessageDecodingConverter.class)) {
      result = convert(JSON_LINES, message);
      errors =
          logs.events().stream()
              .filter(e -> e.getLevel() == Level.ERROR)
              .map(e -> e.getMessage().getFormattedMessage())
              .toList();
    }

    assertEquals(
        List.of(
            "failed to deserialize event: MALFORMED json message: JsonParseException\n" + message),
        errors);

    Throwable cause = result.error().getCause();
    assertEquals(
        new Failure(
            cause,
            "MALFORMED json message: JsonParseException\n"
                + "Action: check that the message is json as the source's encodingType or decoding"
                + " says, and that its compression is set when the message is compressed, or fix"
                + " the producer that wrote it; "
                + RESEND
                + "\n"
                + trace(cause)
                + "\nrecordIndex 2, bytes 8-16",
            message),
        Failure.of(result));
    verify(dataSize).increase(message.value().length, Map.of());
    verify(failures).increase(Map.of());
    verify(inputCount, never()).increase(anyLong(), anyMap());
  }

  @Test
  void corruptCompressionFailsTheMessage() {
    DecodingConfig gzipJson =
        new DecodingConfig(
            List.of(DecompressionCodec.GZIP), new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT));
    SerializedEvent message =
        new SerializedEvent(null, new byte[] {0x1f, (byte) 0x8b, 8, 0, 1, 2, 3}, null);

    ConvertedResult<SerializedEvent> result = convert(gzipJson, message);

    Throwable cause = result.error().getCause();
    assertEquals(
        new Failure(
            cause,
            "CORRUPT_COMPRESSION json message: CorruptCompressionException\n"
                + "Action: check the source's compression setting against the message; otherwise"
                + " the producer wrote damaged compressed data, or bytes after its end; "
                + RESEND
                + "\n"
                + trace(cause),
            message),
        Failure.of(result));
  }

  @Test
  void failedOutcomeTextWithSeveralProblems() {
    Exception cause = new IllegalArgumentException("bad");
    RecordDecoder decoder =
        stub(
            new DecodeResult(
                List.of(record(Map.of("a", 1))),
                List.of(
                    new DecodeOutcome.Failed(
                        ErrorKind.MALFORMED, null, 3, new RawSpan(10, 15), cause, false),
                    new DecodeOutcome.Failed(
                        ErrorKind.MALFORMED, null, 4, new RawSpan(26, 3), cause, false),
                    new DecodeOutcome.Failed(
                        ErrorKind.MALFORMED, null, 5, new RawSpan(30, 3), cause, false))));
    SerializedEvent message = message("x");

    ConvertedResult<SerializedEvent> result =
        new MessageDecodingConverter(
                decoder, JSON_LINES.format(), MessageDecodingConverter.Unit.MESSAGE)
            .convert(message, context);

    assertEquals(
        new Failure(
            cause,
            "MALFORMED json message: IllegalArgumentException\n"
                + "Action: check that the message is json as the source's encodingType or decoding"
                + " says, and that its compression is set when the message is compressed, or fix"
                + " the producer that wrote it; "
                + RESEND
                + "\n"
                + trace(cause)
                + "\nrecordIndex 3, bytes 10-25 (and 2 more problems)",
            message),
        Failure.of(result));
  }

  @Test
  void problemWithoutLocationNamesOnlyTheOthers() {
    Exception cause = new IllegalArgumentException("bad");
    RecordDecoder decoder =
        stub(
            new DecodeResult(
                List.of(),
                List.of(
                    new DecodeOutcome.Failed(
                        ErrorKind.CORRUPT_COMPRESSION, null, 0, null, cause, true),
                    new DecodeOutcome.Failed(
                        ErrorKind.MALFORMED, null, 1, RawSpan.toEnd(0), cause, false))));
    SerializedEvent message = message("x");

    ConvertedResult<SerializedEvent> result =
        new MessageDecodingConverter(
                decoder, JSON_LINES.format(), MessageDecodingConverter.Unit.MESSAGE)
            .convert(message, context);

    assertEquals(
        new Failure(
            cause,
            "CORRUPT_COMPRESSION json message: IllegalArgumentException\n"
                + "Action: check the source's compression setting against the message; otherwise"
                + " the producer wrote damaged compressed data, or bytes after its end; "
                + RESEND
                + "\n"
                + trace(cause)
                + "\nand 1 more problem",
            message),
        Failure.of(result));
  }

  @Test
  void openEndedSpanGivesOnlyTheIndex() {
    Exception cause = new IllegalArgumentException("bad");
    RecordDecoder decoder =
        stub(
            new DecodeResult(
                List.of(),
                List.of(
                    new DecodeOutcome.Failed(
                        ErrorKind.MALFORMED, null, 2, RawSpan.toEnd(5), cause, true))));
    SerializedEvent message = message("x");

    ConvertedResult<SerializedEvent> result =
        new MessageDecodingConverter(
                decoder, JSON_LINES.format(), MessageDecodingConverter.Unit.MESSAGE)
            .convert(message, context);

    assertEquals("recordIndex 2", Failure.of(result).dlqText().lines().toList().getLast());
  }

  @Test
  void oversizedMultilineEvent() {
    DecodingConfig multiline =
        new DecodingConfig(
            List.of(), new TextFormatConfig(null, new FramingConfig.Multiline("^start", null, 2)));
    SerializedEvent message = message("start\na\nb\n");

    ConvertedResult<SerializedEvent> result = convert(multiline, message);

    assertEquals(
        new Failure(
            null,
            "OVERSIZED text message: maxLines\n"
                + "Action: raise maxLines in the source's decoding, or fix its multiline pattern if"
                + " events run together; "
                + RESEND
                + "\na multiline event has more than 2 lines (9 bytes)"
                + "\nrecordIndex 1, bytes 0-9",
            message),
        Failure.of(result));
    verify(failures).increase(Map.of());
  }

  static Stream<Arguments> oversizedKinds() {
    long mib = 1024 * 1024;
    FormatConfig json = JSON_OBJECT.format();
    FormatConfig avro = new AvroFormatConfig(AvroFormatConfig.Container.OCF, null);
    FormatConfig protobuf = new ProtobufFormatConfig("d", "m", ProtobufFormatConfig.Framing.SINGLE);
    return Stream.of(
        Arguments.of(
            json,
            LimitKind.MAX_CODEC_MEMORY_BYTES,
            128 * mib,
            200 * mib,
            "OVERSIZED json message: xz memory\n"
                + "Action: recompress the message with a smaller xz dictionary (the presets -0 to"
                + " -9 need at most 65 MiB); no setting raises this limit; "
                + RESEND
                + "\nxz needs 209715200 bytes of memory, over its limit of 134217728"),
        Arguments.of(
            json,
            LimitKind.MAX_CODEC_MEMORY_BYTES,
            64 * mib,
            200 * mib,
            "OVERSIZED json message: xz memory\n"
                + "Action: give the task more memory (xz may use half the heap, up to 128 MiB), or"
                + " recompress the message with a smaller xz dictionary (the presets -0 to -9 need"
                + " at most 65 MiB); "
                + RESEND
                + "\nxz needs 209715200 bytes of memory, over its limit of 67108864"),
        Arguments.of(
            avro,
            LimitKind.AVRO_XZ_MEMORY,
            128 * mib,
            200 * mib,
            "OVERSIZED avro message: avro xz memory\n"
                + "Action: rewrite the avro file with a lower xz level for its blocks (levels 0 to"
                + " 9 need at most 65 MiB); no setting raises this limit; "
                + RESEND
                + "\nan avro block's xz data needs 209715200 bytes of memory, over its limit of"
                + " 134217728"),
        Arguments.of(
            json,
            LimitKind.ZSTD_WINDOW,
            128 * mib,
            0,
            "OVERSIZED json message: zstd window\n"
                + "Action: compress the zstd data (the message, or an avro file's zstandard blocks)"
                + " without long mode, or with a window of 128 MiB or less (--long=27); no setting"
                + " raises this limit; "
                + RESEND
                + "\nzstd data needs a window over 134217728 bytes"),
        Arguments.of(
            json,
            LimitKind.NATIVE_MEMORY,
            0,
            0,
            "OVERSIZED json message: zstd native memory\n"
                + "Action: the worker ran out of memory outside the Java heap: give the worker more"
                + " memory or run fewer tasks on it; if it was momentary, "
                + RESEND
                + "\nzstd could not allocate native memory"),
        Arguments.of(
            new ParquetFormatConfig(),
            LimitKind.HEAP_SHARE,
            100,
            200,
            "OVERSIZED parquet message: parquet heap share\n"
                + "Action: give the task more memory, or write the file with smaller row groups or"
                + " rows (fewer row groups or columns if the footer is too large); "
                + RESEND
                + "\nparquet needs 200 bytes, over its share of the heap, 100"),
        Arguments.of(
            protobuf,
            LimitKind.FORMAT_MAXIMUM,
            100,
            200,
            "OVERSIZED protobuf message: format maximum\n"
                + "Action: write the file in smaller pieces (avro: smaller blocks; protobuf:"
                + " smaller messages; parquet: a smaller footer); no setting raises this limit; "
                + RESEND
                + "\na protobuf message reached 200 bytes, over the 100 bytes the decoder can hold"
                + " in one piece"));
  }

  @ParameterizedTest
  @MethodSource("oversizedKinds")
  void oversizedText(FormatConfig format, LimitKind limit, long value, long size, String text) {
    SerializedEvent message = message("x");

    ConvertedResult<SerializedEvent> result =
        new MessageDecodingConverter(
                stub(oversized(limit, value, size)), format, MessageDecodingConverter.Unit.MESSAGE)
            .convert(message, context);

    assertEquals(new Failure(null, text + "\nrecordIndex 1", message), Failure.of(result));
  }

  @Test
  void decodesWithoutLimits() {
    RecordDecoder decoder = mock();
    when(decoder.decodeAll(any(), any())).thenReturn(new DecodeResult(List.of(), List.of()));
    SerializedEvent message = message("x");

    new MessageDecodingConverter(
            decoder, JSON_OBJECT.format(), MessageDecodingConverter.Unit.MESSAGE)
        .convert(message, context);

    verify(decoder).decodeAll(same(message.value()), eq(DecodeLimits.UNLIMITED));
  }

  @Test
  void decoderExceptionIsReadFailure() {
    UncheckedIOException cause = new UncheckedIOException(new IOException("boom"));
    RecordDecoder decoder = mock();
    when(decoder.decodeAll(any(), any())).thenThrow(cause);
    SerializedEvent message = message("x");

    ConvertedResult<SerializedEvent> result =
        new MessageDecodingConverter(
                decoder, JSON_OBJECT.format(), MessageDecodingConverter.Unit.MESSAGE)
            .convert(message, context);

    assertEquals(
        new Failure(
            cause,
            "READ_FAILURE json message: IOException\n"
                + "Action: the source could not process the message (the error says why); if"
                + " every message fails like this, check the source's encodingType or decoding; "
                + RESEND
                + "\n"
                + trace(cause),
            message),
        Failure.of(result));
    verify(dataSize).increase(1, Map.of());
    verify(failures).increase(Map.of());
  }

  @Test
  void fileTexts() {
    Exception cause = new IllegalArgumentException("bad");
    SerializedEvent message = message("x");
    MessageDecodingConverter.Unit file = MessageDecodingConverter.Unit.FILE;

    assertEquals(
        new Failure(
            cause,
            "MALFORMED json file: IllegalArgumentException\n"
                + "Action: check that the file is json as the source's encodingType or decoding"
                + " says, and that its compression is set when the file is compressed; fix the"
                + " file and run the job again\n"
                + trace(cause)
                + "\nrecordIndex 1",
            message),
        Failure.of(
            new MessageDecodingConverter(
                    stub(failed(ErrorKind.MALFORMED, cause)), JSON_OBJECT.format(), file)
                .convert(message, context)));
    assertEquals(
        new Failure(
            cause,
            "CORRUPT_COMPRESSION json file: IllegalArgumentException\n"
                + "Action: check the source's compression setting against the file; otherwise the"
                + " file holds damaged compressed data, or bytes after its end; fix the file and"
                + " run the job again\n"
                + trace(cause)
                + "\nrecordIndex 1",
            message),
        Failure.of(
            new MessageDecodingConverter(
                    stub(failed(ErrorKind.CORRUPT_COMPRESSION, cause)), JSON_OBJECT.format(), file)
                .convert(message, context)));

    assertEquals(
        new Failure(
            null,
            "OVERSIZED json file: zstd window\n"
                + "Action: compress the zstd data (the file, or an avro file's zstandard blocks)"
                + " without long mode, or with a window of 128 MiB or less (--long=27); no setting"
                + " raises this limit; fix the file and run the job again\n"
                + "zstd data needs a window over 134217728 bytes\n"
                + "recordIndex 1",
            message),
        Failure.of(
            new MessageDecodingConverter(
                    stub(oversized(LimitKind.ZSTD_WINDOW, 128 * 1024 * 1024, 0)),
                    JSON_OBJECT.format(),
                    file)
                .convert(message, context)));

    RuntimeException thrown = new RuntimeException("boom");
    RecordDecoder throwing = mock();
    when(throwing.decodeAll(any(), any())).thenThrow(thrown);
    ConvertedResult<SerializedEvent> readFailure =
        new MessageDecodingConverter(throwing, JSON_OBJECT.format(), file)
            .convert(message, context);
    assertEquals(
        new Failure(
            thrown,
            "READ_FAILURE json file: RuntimeException\n"
                + "Action: the source could not process the file (the error says why); check the"
                + " source's encodingType or decoding, fix the file and run the job again\n"
                + trace(thrown),
            message),
        Failure.of(readFailure));
  }

  @Test
  void encodingTypeDetectsGzipOnlyWhenAsked() throws IOException {
    JsonFormatConfig object = new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT);
    assertEquals(
        new DecodingConfig(List.of(DecompressionCodec.AUTO), object),
        MessageDecodingConverter.decodingConfig(EncodingType.JSON_OBJECT, null, true));
    assertEquals(
        new DecodingConfig(List.of(), object),
        MessageDecodingConverter.decodingConfig(EncodingType.JSON_OBJECT, null, false));

    SerializedEvent gzipped = new SerializedEvent(null, gzip("{\"a\":1}"), null);
    assertEquals(
        ConvertedResult.success(List.of(record(Map.of("a", 1))), gzipped),
        convert(
            MessageDecodingConverter.decodingConfig(EncodingType.JSON_OBJECT, null, true),
            gzipped));
    assertInstanceOf(
        MessageDecodeException.class,
        convert(
                MessageDecodingConverter.decodingConfig(EncodingType.JSON_OBJECT, null, false),
                gzipped)
            .error());
  }

  @Test
  void decodingIsUsedAsGiven() {
    DecodingConfig decoding =
        new DecodingConfig(List.of(DecompressionCodec.ZSTD), JSON_LINES.format());

    assertEquals(decoding, MessageDecodingConverter.decodingConfig(null, decoding, false));
    assertEquals(decoding, MessageDecodingConverter.decodingConfig(null, decoding, true));
  }

  @Test
  void exactlyOneOfEncodingTypeAndDecoding() {
    assertEquals(
        "exactly one of encodingType and decoding must be set",
        assertThrows(
                IllegalArgumentException.class,
                () -> MessageDecodingConverter.decodingConfig(null, null, true))
            .getMessage());
    assertEquals(
        "exactly one of encodingType and decoding must be set",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    MessageDecodingConverter.decodingConfig(
                        EncodingType.JSON_OBJECT, JSON_OBJECT, true))
            .getMessage());
  }

  /** A failed result, with the parts of its {@link MessageDecodeException} that matter. */
  private record Failure(Throwable cause, String dlqText, SerializedEvent sourceRecord) {
    static Failure of(ConvertedResult<SerializedEvent> result) {
      assertNull(result.transformedData());
      MessageDecodeException error = (MessageDecodeException) result.error();
      assertEquals(error.getDlqText().lines().findFirst().orElseThrow(), error.getMessage());
      return new Failure(error.getCause(), error.getDlqText(), result.sourceRecord());
    }
  }

  private ConvertedResult<SerializedEvent> convert(DecodingConfig config, SerializedEvent message) {
    return new MessageDecodingConverter(config, MessageDecodingConverter.Unit.MESSAGE)
        .convert(message, context);
  }

  private static SerializedEvent message(String value) {
    return new SerializedEvent(null, value.getBytes(StandardCharsets.UTF_8), null);
  }

  private static RecordFleakData record(Map<String, Object> fields) {
    return (RecordFleakData) FleakData.wrap(fields);
  }

  private static DecodeResult failed(ErrorKind kind, Exception cause) {
    return new DecodeResult(
        List.of(), List.of(new DecodeOutcome.Failed(kind, null, 1, null, cause, false)));
  }

  private static DecodeResult oversized(LimitKind limit, long value, long size) {
    return new DecodeResult(
        List.of(),
        List.of(new DecodeOutcome.Oversized(1, 1, null, limit, value, size, new byte[0], true)));
  }

  private static String trace(Throwable error) {
    return ExceptionUtils.getStackTrace(error).stripTrailing();
  }

  private static RecordDecoder stub(DecodeResult result) {
    return new RecordDecoder() {
      @Override
      public RecordCursor open(ByteInput input, DecodeLimits limits) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DecodeResult decodeAll(byte[] bytes, DecodeLimits limits) {
        return result;
      }
    };
  }

  private static byte[] gzip(String text) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
