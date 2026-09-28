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
package io.fleak.zephflow.lib.serdes.des;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Streamed decoding reads records one at a time from an {@link InputStream}, so a payload never has
 * to fit in memory. These tests pin which formats take which path and how a streamed parse reports
 * malformed input: records before the fault are kept, and the fault is reported rather than thrown.
 */
class StreamedDecodingTest {

  private record Streamed(
      List<Map<String, Object>> records, List<DeserializationOutcome.RecordError> errors) {}

  private static FleakDeserializer<?> deserializer(EncodingType encodingType) {
    return DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();
  }

  private static Streamed stream(EncodingType encodingType, String payload) throws IOException {
    return stream(encodingType, new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
  }

  private static Streamed stream(EncodingType encodingType, InputStream input) throws IOException {
    List<Map<String, Object>> records = new ArrayList<>();
    List<DeserializationOutcome.RecordError> errors = new ArrayList<>();
    deserializer(encodingType)
        .deserializeStream(input, record -> records.add(record.unwrap()), errors::add);
    return new Streamed(records, errors);
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"CSV", "JSON_ARRAY"})
  void recordSequenceFormatsStreamFromTheInput(EncodingType encodingType) {
    assertTrue(deserializer(encodingType).supportsStreamedPayloads());
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"JSON_OBJECT_LINE", "STRING_LINE"})
  void lineDelimitedFormatsAreReadInNewlineAlignedChunks(EncodingType encodingType) {
    assertTrue(deserializer(encodingType).supportsChunkedPayloads());
    assertFalse(deserializer(encodingType).supportsStreamedPayloads());
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"JSON_OBJECT", "TEXT", "XML"})
  void singleDocumentFormatsNeedTheWholePayload(EncodingType encodingType) {
    assertFalse(deserializer(encodingType).supportsChunkedPayloads());
    assertFalse(deserializer(encodingType).supportsStreamedPayloads());
  }

  @Test
  void csvStreamsRowsIncludingAQuotedFieldThatSpansLines() throws Exception {
    Streamed streamed = stream(EncodingType.CSV, "a,b\n1,\"x\ny\"\n2,z\n");

    assertEquals(
        List.of(Map.of("a", "1", "b", "x\ny"), Map.of("a", "2", "b", "z")), streamed.records());
    assertEquals(List.of(), streamed.errors());
  }

  @Test
  void csvReportsAShortRowAndKeepsReadingTheRows() throws Exception {
    Streamed streamed = stream(EncodingType.CSV, "a,b\n1,2\n3\n4,5\n");

    assertEquals(
        List.of(Map.of("a", "1", "b", "2"), Map.of("a", "4", "b", "5")), streamed.records());
    assertEquals(1, streamed.errors().size());
    assertEquals(2, streamed.errors().getFirst().recordIndex(), "the second data row is short");
    assertEquals("3", new String(streamed.errors().getFirst().rawRecord(), StandardCharsets.UTF_8));
  }

  @Test
  void csvKeepsTheRowsBeforeUnterminatedQuotingAndReportsTheRest() throws Exception {
    Streamed streamed = stream(EncodingType.CSV, "a\n1\n\"never closed\n2\n");

    assertEquals(List.of(Map.of("a", "1")), streamed.records());
    assertEquals(1, streamed.errors().size());
    assertEquals(2, streamed.errors().getFirst().recordIndex());
  }

  @Test
  void anEmptyCsvHasNoRecordsAndNoErrors() throws Exception {
    Streamed streamed = stream(EncodingType.CSV, "");

    assertEquals(List.of(), streamed.records());
    assertEquals(List.of(), streamed.errors());
  }

  @Test
  void jsonArrayStreamsItsElements() throws Exception {
    Streamed streamed = stream(EncodingType.JSON_ARRAY, "[{\"v\":1},{\"v\":{\"n\":2}}]");

    assertEquals(List.of(Map.of("v", 1L), Map.of("v", Map.of("n", 2L))), streamed.records());
    assertEquals(List.of(), streamed.errors());
  }

  @Test
  void jsonArrayReportsANonObjectElementAndKeepsReading() throws Exception {
    Streamed streamed = stream(EncodingType.JSON_ARRAY, "[{\"v\":1},2,{\"v\":3}]");

    assertEquals(List.of(Map.of("v", 1L), Map.of("v", 3L)), streamed.records());
    assertEquals(1, streamed.errors().size());
    assertEquals(2, streamed.errors().getFirst().recordIndex());
    assertEquals("2", new String(streamed.errors().getFirst().rawRecord(), StandardCharsets.UTF_8));
  }

  @Test
  void jsonArrayKeepsTheElementsBeforeASyntaxErrorAndReportsTheRest() throws Exception {
    Streamed streamed = stream(EncodingType.JSON_ARRAY, "[{\"v\":1},{\"v\":2},{\"v\": nope}]");

    assertEquals(List.of(Map.of("v", 1L), Map.of("v", 2L)), streamed.records());
    assertEquals(1, streamed.errors().size());
    assertEquals(3, streamed.errors().getFirst().recordIndex());
  }

  @Test
  void jsonArrayReportsATruncatedArray() throws Exception {
    Streamed streamed = stream(EncodingType.JSON_ARRAY, "[{\"v\":1},");

    assertEquals(List.of(Map.of("v", 1L)), streamed.records());
    assertEquals(1, streamed.errors().size());
  }

  @Test
  void jsonArrayReportsADocumentThatIsNotAnArray() throws Exception {
    Streamed streamed = stream(EncodingType.JSON_ARRAY, "{\"v\":1}");

    assertEquals(List.of(), streamed.records());
    assertEquals(1, streamed.errors().size());
  }

  @Test
  void jsonArrayReportsContentAfterTheArray() throws Exception {
    Streamed streamed = stream(EncodingType.JSON_ARRAY, "[{\"v\":1}] trailing");

    assertEquals(List.of(Map.of("v", 1L)), streamed.records());
    assertEquals(1, streamed.errors().size());
  }

  /** Throws a plain I/O failure once the payload's bytes are used up. */
  private static InputStream failingAfter(String payload) {
    byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
    return new InputStream() {
      private int position = 0;

      @Override
      public int read() throws IOException {
        if (position >= bytes.length) {
          throw new IOException("connection reset");
        }
        return bytes[position++];
      }
    };
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"CSV", "JSON_ARRAY"})
  void aReadFailureIsThrownRatherThanReportedAsMalformedInput(EncodingType encodingType) {
    String payload = encodingType == EncodingType.CSV ? "a\n1\n" : "[{\"v\":1},";

    IOException thrown =
        assertThrows(IOException.class, () -> stream(encodingType, failingAfter(payload)));
    assertTrue(thrown.getMessage().contains("connection reset"), thrown.getMessage());
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"CSV", "JSON_ARRAY"})
  void aConsumerFailureIsThrownRatherThanReportedAsMalformedInput(EncodingType encodingType) {
    String payload = encodingType == EncodingType.CSV ? "a\n1\n2\n" : "[{\"v\":1},{\"v\":2}]";
    List<DeserializationOutcome.RecordError> errors = new ArrayList<>();

    assertThrows(
        IllegalStateException.class,
        () ->
            deserializer(encodingType)
                .deserializeStream(
                    new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)),
                    (RecordFleakData record) -> {
                      throw new IllegalStateException("downstream");
                    },
                    errors::add));
    assertEquals(List.of(), errors);
  }
}
