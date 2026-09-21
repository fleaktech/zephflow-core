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

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class IncrementalDecodingTest {
  private static final Map<EncodingType, String> INPUTS =
      Map.of(
          EncodingType.CSV, "a,b\n1,one\n2,two\n",
          EncodingType.JSON_OBJECT, "{\"a\":1}",
          EncodingType.JSON_ARRAY, "[{\"a\":1},{\"a\":2}]",
          EncodingType.JSON_OBJECT_LINE, "{\"a\":1}\r\n[{\"a\":2},{\"a\":3}]\n",
          EncodingType.STRING_LINE, "one\r\n\r\ntwo\n",
          EncodingType.TEXT, "one\r\ntwo\n",
          EncodingType.XML, "<event><a>1</a></event>");

  private static SerializedEvent event(String input) {
    return new SerializedEvent(null, input.getBytes(StandardCharsets.UTF_8), null);
  }

  private static FleakDeserializer<?> decoder(EncodingType type) {
    return DeserializerFactory.createDeserializerFactory(type).createDeserializer();
  }

  @ParameterizedTest
  @EnumSource(EncodingType.class)
  void everyFactoryHasExplicitIncrementalCoverageAndMatchesLegacy(EncodingType type)
      throws Exception {
    if (type == EncodingType.PARQUET) {
      assertThrows(UnsupportedOperationException.class, () -> decoder(type));
      return;
    }
    assertTrue(INPUTS.containsKey(type), "New encoding requires an explicit fixture");
    var event = event(INPUTS.get(type));
    var deserializer = decoder(type);
    var outcomes = new ArrayList<IncrementalRecord>();
    deserializer.deserializeIncrementally(event, outcomes::add, () -> false);
    assertTrue(outcomes.stream().allMatch(o -> o.error() == null));
    assertEquals(
        deserializer.deserialize(event).stream().map(r -> r.unwrap()).toList(),
        outcomes.stream().map(o -> o.record().unwrap()).toList());
    for (int i = 0; i < outcomes.size(); i++) {
      assertEquals(i + 1, outcomes.get(i).subrecordIndex());
      assertSame(event.value(), outcomes.get(i).rawPayload());
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"CSV", "JSON_ARRAY", "JSON_OBJECT_LINE", "STRING_LINE"})
  void largeFanoutStopsAfterFirstOutput(EncodingType type) {
    String input =
        switch (type) {
          case CSV -> "a\n" + "x\n".repeat(20_000);
          case JSON_ARRAY, JSON_OBJECT_LINE -> "[" + "{\"a\":1},".repeat(19_999) + "{\"a\":2}]";
          case STRING_LINE -> "x\n".repeat(20_000);
          default -> throw new AssertionError(type);
        };
    var stopped = new AtomicBoolean();
    var output = new ArrayList<IncrementalRecord>();
    decoder(type)
        .deserializeIncrementally(
            event(input),
            o -> {
              output.add(o);
              stopped.set(true);
            },
            stopped::get);
    assertEquals(1, output.size());
    assertNull(output.getFirst().error());
  }

  @Test
  void cancellationInsideArrayValidationEmitsNeitherSuccessNorError() {
    var checks = new AtomicInteger();
    var output = new ArrayList<IncrementalRecord>();
    decoder(EncodingType.JSON_OBJECT_LINE)
        .deserializeIncrementally(
            event("[" + "{\"a\":1},".repeat(20_000) + "{\"a\":2}]"),
            output::add,
            () -> checks.incrementAndGet() > 100);
    assertTrue(output.isEmpty());
    assertTrue(checks.get() < 120, "Stop must interrupt the validation token loop");
  }

  @Test
  void mixedLineErrorsKeepEncounterOrderAndOriginalBytesIncludingInvalidUtf8() {
    byte[] bytes =
        new byte[] {
          '{',
          '"',
          'a',
          '"',
          ':',
          '1',
          '}',
          '\r',
          '\n',
          (byte) 0xc3,
          '\r',
          '\n',
          '{',
          '"',
          'a',
          '"',
          ':',
          '2',
          '}'
        };
    var output = new ArrayList<IncrementalRecord>();
    decoder(EncodingType.JSON_OBJECT_LINE)
        .deserializeIncrementally(new SerializedEvent(null, bytes, null), output::add, () -> false);
    assertEquals(3, output.size());
    assertNull(output.get(0).error());
    assertNotNull(output.get(1).error());
    assertNull(output.get(2).error());
    assertEquals(List.of(1, 2, 3), output.stream().map(IncrementalRecord::subrecordIndex).toList());
    var failed = output.get(1);
    assertSame(bytes, failed.rawPayload());
    assertArrayEquals(
        new byte[] {(byte) 0xc3},
        Arrays.copyOfRange(bytes, failed.rawOffset(), failed.rawOffset() + failed.rawLength()));
  }

  @Test
  void invalidLastArrayElementRejectsEntireLineAndContinuesNextLine() {
    var output = new ArrayList<IncrementalRecord>();
    decoder(EncodingType.JSON_OBJECT_LINE)
        .deserializeIncrementally(event("[{\"a\":1},7]\n{\"a\":2}"), output::add, () -> false);
    assertEquals(2, output.size());
    assertNotNull(output.getFirst().error());
    assertEquals(2L, output.getLast().record().unwrap().get("a"));
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"CSV", "JSON_ARRAY"})
  void lateDocumentFailureDoesNotLeakEarlierRows(EncodingType type) {
    String input = type == EncodingType.CSV ? "a,b\n1,ok\n2\n" : "[{\"a\":1},{\"a\":2},7]";
    var output = new ArrayList<IncrementalRecord>();
    var deserializer = decoder(type);
    assertTrue(deserializer.deserializeWithErrors(event(input)).hasErrors());
    deserializer.deserializeIncrementally(event(input), output::add, () -> false);
    assertEquals(1, output.size());
    assertNotNull(output.getFirst().error());
    assertEquals(-1, output.getFirst().subrecordIndex());
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"PARQUET"},
      mode = EnumSource.Mode.EXCLUDE)
  void downstreamFailureIsNeverReclassifiedAsDecodeError(EncodingType type) {
    RuntimeException failure = new RuntimeException("storage failed");
    AtomicInteger calls = new AtomicInteger();
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                decoder(type)
                    .deserializeIncrementally(
                        event(INPUTS.get(type)),
                        o -> {
                          calls.incrementAndGet();
                          throw failure;
                        },
                        () -> false));
    assertSame(failure, thrown);
    assertEquals(1, calls.get());
  }

  @Test
  void trailingJsonIsAnAtomicError() {
    for (EncodingType type : List.of(EncodingType.JSON_ARRAY, EncodingType.JSON_OBJECT_LINE)) {
      var output = new ArrayList<IncrementalRecord>();
      decoder(type).deserializeIncrementally(event("[{\"a\":1}] true"), output::add, () -> false);
      assertEquals(1, output.size());
      assertNotNull(output.getFirst().error());
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = EncodingType.class,
      names = {"PARQUET"},
      mode = EnumSource.Mode.EXCLUDE)
  void absentTransportPayloadIsAnOrderedErrorWithoutInventingRawBytes(EncodingType type) {
    var deserializer = decoder(type);
    var output = new ArrayList<IncrementalRecord>();
    deserializer.deserializeIncrementally(event(INPUTS.get(type)), output::add, () -> false);
    int firstMessageCount = output.size();
    deserializer.deserializeIncrementally(
        new SerializedEvent(null, null, null), output::add, () -> false);
    deserializer.deserializeIncrementally(event(INPUTS.get(type)), output::add, () -> false);
    assertEquals(firstMessageCount * 2 + 1, output.size());
    IncrementalRecord tombstone = output.get(firstMessageCount);
    assertNull(tombstone.record());
    assertNull(tombstone.rawPayload());
    assertNotNull(tombstone.error());
    assertEquals(-1, tombstone.subrecordIndex());
    assertEquals(-1, tombstone.rawOffset());
    assertNull(output.getLast().error());
  }
}
