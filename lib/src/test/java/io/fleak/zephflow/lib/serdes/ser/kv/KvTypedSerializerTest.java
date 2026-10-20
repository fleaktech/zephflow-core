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
package io.fleak.zephflow.lib.serdes.ser.kv;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.TextBlobFileWriter;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.compression.CompressorFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KvTypedSerializerTest {

  private static String serialize(KvSerializerOptions options, List<Map<String, Object>> events) {
    byte[] bytes = new KvTypedSerializer(options).serializeToMultipleTypedEvent(events);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static Map<String, Object> event(Object... kvs) {
    Map<String, Object> event = new LinkedHashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      event.put((String) kvs[i], kvs[i + 1]);
    }
    return event;
  }

  @Test
  void allFieldsSortedByName_whenNoFieldsPinned() {
    String out =
        serialize(
            KvSerializerOptions.DEFAULT,
            List.of(event("src_ip", "10.0.4.17", "port", 443, "allowed", true)));
    assertEquals("allowed=true port=443 src_ip=10.0.4.17", out);
  }

  @Test
  void pinnedFieldsSetOrderAndSkipNullOrMissing() {
    Map<String, Object> e = event("action", "allow", "src_ip", "10.0.4.17", "user", null);
    String out =
        serialize(
            new KvSerializerOptions(List.of("src_ip", "dst_ip", "user", "action"), null, false),
            List.of(e));
    assertEquals("src_ip=10.0.4.17 action=allow", out);
  }

  @Test
  void oneLinePerEvent() {
    String out =
        serialize(KvSerializerOptions.DEFAULT, List.of(event("a", 1), event("a", 2, "b", "x")));
    assertEquals("a=1\na=2 b=x", out);
  }

  @Test
  void quotesAndEscapesValuesThatWouldBreakParsing() {
    Map<String, Object> e =
        event(
            "msg", "user logged in",
            "q", "say \"hi\"",
            "path", "C:\\Windows",
            "eq", "a=b",
            "multi", "line1\nline2",
            "empty", "");
    assertEquals(
        "empty=\"\" eq=\"a=b\" msg=\"user logged in\" multi=\"line1\\nline2\""
            + " path=\"C:\\\\Windows\" q=\"say \\\"hi\\\"\"",
        serialize(KvSerializerOptions.DEFAULT, List.of(e)));
  }

  @Test
  void customPairDelimiterQuotesValuesContainingIt() {
    String out =
        serialize(
            new KvSerializerOptions(null, ", ", false), List.of(event("a", "x, y", "b", "plain")));
    assertEquals("a=\"x, y\", b=plain", out);
  }

  @Test
  void cleanFieldsReplacesNonAlphanumericKeyChars() {
    Map<String, Object> e = event("event.id", 4624, "Logon-Type", 3);
    assertEquals(
        "event_id=4624 Logon_Type=3",
        serialize(new KvSerializerOptions(null, null, true), List.of(e)));
    assertEquals("event.id=4624 Logon-Type=3", serialize(KvSerializerOptions.DEFAULT, List.of(e)));
  }

  @Test
  void nestedValuesAreQuotedJson() {
    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("os", "win");
    Map<String, Object> e = event("device", nested, "tags", Arrays.asList("a", "b"));
    assertEquals(
        "device=\"{\\\"os\\\":\\\"win\\\"}\" tags=\"[\\\"a\\\",\\\"b\\\"]\"",
        serialize(KvSerializerOptions.DEFAULT, List.of(e)));
  }

  @Test
  void eventWithNoConfiguredFieldsSerializesToNothing() {
    KvTypedSerializer serializer =
        new KvTypedSerializer(new KvSerializerOptions(List.of("missing"), null, false));
    assertEquals("", serializer.toLine(event("other", 1)));
    assertEquals(0, serializer.serializeToMultipleTypedEvent(List.of(event("other", 1))).length);
  }

  @Test
  void eventWithOnlyNullValuesSerializesToNothingWhenUnpinned() {
    assertEquals("", new KvTypedSerializer(KvSerializerOptions.DEFAULT).toLine(event("a", null)));
  }

  @Test
  void emptyEventsAddNoBlankLinesToABatch() {
    assertEquals(
        "src_ip=1\nsrc_ip=3",
        serialize(
            new KvSerializerOptions(List.of("src_ip"), null, false),
            List.of(event("src_ip", 1), event("other", 2), event("src_ip", 3), event("x", 4))));
  }

  @Test
  void s3RecordValidationAcceptsEventsWithNoPairs() {
    var serializer =
        SerializerFactory.createSerializerFactory(
                EncodingType.KV, null, new KvSerializerOptions(List.of("src_ip"), null, false))
            .createSerializer();
    var writer =
        new TextBlobFileWriter(serializer, EncodingType.KV, CompressorFactory.getCompressor(null));
    assertDoesNotThrow(
        () -> writer.validateRecord((RecordFleakData) FleakData.wrap(Map.of("other", "x"))));
  }

  @Test
  void factoryRoundTripsThroughFleakSerializer() throws Exception {
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("user", "alice smith"));
    byte[] bytes =
        SerializerFactory.createSerializerFactory(
                EncodingType.KV, null, KvSerializerOptions.DEFAULT)
            .createSerializer()
            .serialize(List.of(record))
            .value();
    assertEquals("user=\"alice smith\"", new String(bytes, StandardCharsets.UTF_8));
  }

  @Test
  void optionsNormalizeAndValidate() {
    KvSerializerOptions options = KvSerializerOptions.of(List.of(" a ", "b"), "", null);
    assertEquals(List.of("a", "b"), options.fields());
    assertEquals(" ", options.pairDelimiter());
    assertFalse(options.cleanFields());

    assertThrows(
        IllegalArgumentException.class,
        () -> new KvSerializerOptions(List.of("a", " "), null, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> new KvSerializerOptions(List.of("a", "a "), null, false));
    assertThrows(IllegalArgumentException.class, () -> new KvSerializerOptions(null, "=", false));
    assertThrows(IllegalArgumentException.class, () -> new KvSerializerOptions(null, "\n", false));
  }

  @Test
  void blobFilesUseRawExtension() {
    var serializer =
        SerializerFactory.createSerializerFactory(
                EncodingType.KV, null, KvSerializerOptions.DEFAULT)
            .createSerializer();
    assertEquals(
        "raw",
        new TextBlobFileWriter(serializer, EncodingType.KV, CompressorFactory.getCompressor(null))
            .getFileExtension());
    assertEquals(
        "raw.gz",
        new TextBlobFileWriter(
                serializer, EncodingType.KV, CompressorFactory.getCompressor(CompressionType.GZIP))
            .getFileExtension());
  }

  private static String serializeRecords(
      KvSerializerOptions options, List<Map<String, Object>> events) throws Exception {
    List<RecordFleakData> records =
        events.stream().map(e -> (RecordFleakData) FleakData.wrap(e)).toList();
    byte[] bytes =
        SerializerFactory.createSerializerFactory(EncodingType.KV, null, options)
            .createSerializer()
            .serialize(records)
            .value();
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Test
  void numbersAndBooleansFromEngineRecordsKeepTheirJsonLikeForm() throws Exception {
    Map<String, Object> e =
        event("port", 443, "neg", -3L, "ratio", 1.5, "big", 1.0e10, "ok", false);
    assertEquals(
        "port=443 neg=-3 ratio=1.5 big=1.0E10 ok=false",
        serializeRecords(
            new KvSerializerOptions(List.of("port", "neg", "ratio", "big", "ok"), null, false),
            List.of(e)));
  }

  @Test
  void multipleRecordsThroughFactoryAreNewlineSeparatedWithoutTrailingNewline() throws Exception {
    assertEquals(
        "a=1\na=2",
        serializeRecords(KvSerializerOptions.DEFAULT, List.of(event("a", 1), event("a", 2))));
  }

  @Test
  void emptyEventListYieldsNoBytes() {
    assertEquals("", serialize(KvSerializerOptions.DEFAULT, List.of()));
  }

  @Test
  void tabIsQuotedButKeptAndCarriageReturnIsEscaped() {
    assertEquals(
        "cr=\"x\\ry\" t=\"a\tb\"",
        serialize(KvSerializerOptions.DEFAULT, List.of(event("t", "a\tb", "cr", "x\ry"))));
  }

  @Test
  void nonAsciiIsWrittenAsUtf8() {
    byte[] bytes =
        new KvTypedSerializer(KvSerializerOptions.DEFAULT)
            .serializeToMultipleTypedEvent(List.of(event("user", "张伟", "city", "Zürich")));
    assertEquals("city=Zürich user=张伟", new String(bytes, StandardCharsets.UTF_8));
  }

  @Test
  void tabPairDelimiterQuotesValuesContainingTabs() {
    assertEquals(
        "a=1\tb=\"x\ty\"",
        serialize(new KvSerializerOptions(null, "\t", false), List.of(event("a", 1, "b", "x\ty"))));
  }

  @Test
  void nonWhitespaceDelimiterQuotesValuesContainingIt() {
    assertEquals(
        "a=\"x|y\"|b=\"|\"|c=plain",
        serialize(
            new KvSerializerOptions(null, "|", false),
            List.of(event("a", "x|y", "b", "|", "c", "plain"))));
  }

  @Test
  void keysThatWouldBreakParsingAreSanitizedEvenWithoutCleanFields() {
    Map<String, Object> e =
        event(
            "user name", "a",
            "k=v", "b",
            "q\"k", "c",
            "", "d",
            "event.id", "e");
    assertEquals(
        "_=d event.id=e k_v=b q_k=c user_name=a",
        serialize(KvSerializerOptions.DEFAULT, List.of(e)));
  }

  @Test
  void keysContainingANonWhitespaceDelimiterAreSanitized() {
    assertEquals(
        "a_b=1,c=2",
        serialize(new KvSerializerOptions(null, ",", false), List.of(event("a,b", 1, "c", 2))));
  }

  @Test
  void cleanFieldsKeepsKeysThatCollideAfterCleaning() {
    assertEquals(
        "a_b=2 a_b=1",
        serialize(new KvSerializerOptions(null, null, true), List.of(event("a.b", 1, "a-b", 2))));
  }

  @Test
  void pinnedFieldsAreLookedUpByOriginalNameAndCleanedOnOutput() {
    Map<String, Object> e = event("event.id", 4624, "Logon-Type", 3, "other", "x");
    assertEquals(
        "Logon_Type=3 event_id=4624",
        serialize(
            new KvSerializerOptions(List.of("Logon-Type", "event.id"), null, true), List.of(e)));
  }

  @Test
  void emptyAndNullHoldingNestedValues() {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("a", null);
    Map<String, Object> e = event("m", new LinkedHashMap<>(), "l", List.of(), "n", inner);
    assertEquals(
        "l=[] m={} n=\"{\\\"a\\\":null}\"", serialize(KvSerializerOptions.DEFAULT, List.of(e)));
  }

  @Test
  void emptyFieldListMeansEveryField() {
    KvSerializerOptions options = new KvSerializerOptions(List.of(), null, false);
    assertFalse(options.hasPinnedFields());
    assertEquals("a=1 b=2", serialize(options, List.of(event("a", 1, "b", 2))));
  }

  @Test
  void optionsFromNullConfigUseDefaults() {
    KvSerializerOptions options = KvSerializerOptions.of(null, null, null);
    assertNull(options.fields());
    assertEquals(" ", options.pairDelimiter());
    assertFalse(options.cleanFields());
    assertFalse(options.hasPinnedFields());
  }

  @Test
  void optionsRejectEveryDelimiterCharacterThatBreaksParsing() {
    for (String delimiter : List.of("=", "a=b", "\"", "\\", "\r", " \n")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new KvSerializerOptions(null, delimiter, false),
          "delimiter " + delimiter);
    }
    assertDoesNotThrow(() -> new KvSerializerOptions(null, "; ", false));
  }

  @Test
  void kvIsOptInPerSinkAndNotDeserializable() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SerializerFactory.validateEncodingType(EncodingType.KV));
    assertNotNull(
        SerializerFactory.createSerializerFactory(
            EncodingType.KV, null, KvSerializerOptions.DEFAULT));
    assertThrows(
        UnsupportedOperationException.class,
        () -> DeserializerFactory.createDeserializerFactory(EncodingType.KV));
  }

  @Test
  void fieldNamesDropInvisibleCharsAndNonBreakingSpaces() {
    KvSerializerOptions options =
        new KvSerializerOptions(List.of("src_ip\u200B", "\u00A0action\u00A0"), null, false);
    assertEquals(List.of("src_ip", "action"), options.fields());
    assertEquals(
        "src_ip=1.2.3.4 action=allow",
        serialize(options, List.of(event("action", "allow", "src_ip", "1.2.3.4"))));
  }

  @Test
  void unpinnedKeysSortIgnoringCaseAndKeepCaseVariants() {
    assertEquals(
        "A=1 a=2 action=x Logon_Type=3",
        serialize(
            KvSerializerOptions.DEFAULT,
            List.of(event("Logon_Type", 3, "a", 2, "action", "x", "A", 1))));
  }
}
