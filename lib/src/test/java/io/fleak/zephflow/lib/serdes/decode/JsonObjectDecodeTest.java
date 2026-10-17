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

import java.util.List;
import org.junit.jupiter.api.Test;

class JsonObjectDecodeTest {

  private static final String OBJECT = "{\"format\":{\"type\":\"json\",\"mode\":\"object\"}}";

  @Test
  void decodesOneObject() {
    assertEquals(
        List.of(new Ok(json("{\"a\":{\"b\":[1,true,null]}}"), 1, new RawSpan(0, 25))),
        decode(OBJECT, utf8("{\"a\":{\"b\":[1,true,null]}}")));
  }

  @Test
  void malformedDocumentsKeepTheWholeRaw() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED, "{\"a\":", -1, new RawSpan(0, 5), "JsonEOFException", true)),
        decode(OBJECT, utf8("{\"a\":")));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "{\"a\":1}\n{\"b\":2}",
                -1,
                new RawSpan(0, 15),
                "IllegalArgumentException",
                true)),
        decode(OBJECT, utf8("{\"a\":1}\n{\"b\":2}")));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "[1]",
                -1,
                new RawSpan(0, 3),
                "IllegalArgumentException",
                true)),
        decode(OBJECT, utf8("[1]")));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "  ",
                -1,
                new RawSpan(0, 2),
                "IllegalArgumentException",
                true)),
        decode(OBJECT, utf8("  ")));
  }

  @Test
  void emptyInputHasNoOutcomes() {
    assertEquals(List.of(), decode(OBJECT, new byte[0]));
  }

  @Test
  void documentLimitIsExact() {
    String doc = "{\"a\":\"xxxx\"}";
    assertEquals(
        List.of(new Ok(json(doc), 1, new RawSpan(0, 12))),
        decode(decoder(OBJECT), ByteInput.ofBytes(utf8(doc)), new DecodeLimits(0, 12)));
    assertEquals(
        List.of(new Big(-1, RawSpan.toEnd(0), 12, 12, true)),
        decode(decoder(OBJECT), ByteInput.ofBytes(utf8(doc)), new DecodeLimits(0, 11)));
  }
}
