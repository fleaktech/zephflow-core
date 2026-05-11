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
package io.fleak.zephflow.lib.commands.piimask;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiPathWriterTest {

  /** A rewriter that uppercases the input — easy to verify mutation. */
  private static final PiiPathWriter.StringRewriter UPPER =
      s -> new PiiPathWriter.RewriteResult(s.toUpperCase(), 1);

  @Test
  void rewritesTopLevelStringLeaf() {
    RecordFleakData rec = record(Map.of("foo", str("hello")));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.foo"), UPPER);
    assertEquals(1, n);
    assertEquals(
        "HELLO", ((StringPrimitiveFleakData) rec.getPayload().get("foo")).getStringValue());
  }

  @Test
  void rewritesNestedStringLeaf() {
    RecordFleakData rec = record(Map.of("user", record(Map.of("email", str("a@b.com")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.user.email"), UPPER);
    assertEquals(1, n);
    RecordFleakData inner = (RecordFleakData) rec.getPayload().get("user");
    assertEquals(
        "A@B.COM", ((StringPrimitiveFleakData) inner.getPayload().get("email")).getStringValue());
  }

  @Test
  void rewritesEachStringInArrayLeaf() {
    RecordFleakData rec =
        record(Map.of("recipients", new ArrayFleakData(List.of(str("a"), str("b")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.recipients"), UPPER);
    assertEquals(2, n);
    ArrayFleakData arr = (ArrayFleakData) rec.getPayload().get("recipients");
    assertEquals("A", ((StringPrimitiveFleakData) arr.getArrayPayload().get(0)).getStringValue());
    assertEquals("B", ((StringPrimitiveFleakData) arr.getArrayPayload().get(1)).getStringValue());
  }

  @Test
  void skipsNonStringElementsInArray() {
    RecordFleakData rec =
        record(
            Map.of(
                "mixed",
                new ArrayFleakData(
                    List.of(
                        str("a"),
                        new NumberPrimitiveFleakData(1.0, NumberPrimitiveFleakData.NumberType.LONG),
                        str("c")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.mixed"), UPPER);
    assertEquals(2, n);
    ArrayFleakData arr = (ArrayFleakData) rec.getPayload().get("mixed");
    assertEquals("A", ((StringPrimitiveFleakData) arr.getArrayPayload().get(0)).getStringValue());
    assertInstanceOf(NumberPrimitiveFleakData.class, arr.getArrayPayload().get(1));
    assertEquals("C", ((StringPrimitiveFleakData) arr.getArrayPayload().get(2)).getStringValue());
  }

  @Test
  void skipsMissingTopLevelKey() {
    RecordFleakData rec = record(Map.of("foo", str("hello")));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.bar"), UPPER);
    assertEquals(0, n);
    assertEquals(
        "hello", ((StringPrimitiveFleakData) rec.getPayload().get("foo")).getStringValue());
  }

  @Test
  void skipsMissingNestedKey() {
    RecordFleakData rec = record(Map.of("user", record(Map.of("name", str("dan")))));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.user.email"), UPPER);
    assertEquals(0, n);
  }

  @Test
  void skipsWhenIntermediateIsNotRecord() {
    RecordFleakData rec = record(Map.of("user", str("not-a-record")));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.user.email"), UPPER);
    assertEquals(0, n);
    assertEquals(
        "not-a-record", ((StringPrimitiveFleakData) rec.getPayload().get("user")).getStringValue());
  }

  @Test
  void skipsWhenLeafIsNonStringScalar() {
    RecordFleakData rec =
        record(
            Map.of(
                "count",
                new NumberPrimitiveFleakData(42.0, NumberPrimitiveFleakData.NumberType.LONG)));
    int n = PiiPathWriter.rewrite(rec, DottedPath.parse("$.count"), UPPER);
    assertEquals(0, n);
  }

  private static StringPrimitiveFleakData str(String s) {
    return new StringPrimitiveFleakData(s);
  }

  private static RecordFleakData record(Map<String, FleakData> payload) {
    return new RecordFleakData(new HashMap<>(payload));
  }
}
