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
package io.fleak.zephflow.evaluator;

import org.apache.commons.cli.ParseException;
import org.junit.jupiter.api.Test;

/** Created by bolei on 1/10/25 */
class MainTest {

  @Test
  void testMain() throws ParseException {
    String[] args =
        new String[] {
          "-t",
          "EVAL",
          /* eval expression:
          case(
             $.age > 21 => duration_str_to_mills($.duration_str),
             $.age > 20 => str_split($.address.city, ' '),
             _ => 100
           )
           * */
          "-el",
          "WyInJyIsICIkLmxldmVsIiwgIidzZXJ2ZXInIiwgIidodHRwJyIsICJjYXNlKCQucmVhc29uID09ICdUQ1AgUmVzZXQtSScgPT4gMiwgXyA9PiAxKSIsICJwYXJzZV9pbnQoJC5zb3VyY2VfcG9ydCkiLCAiY2FzZSgkLnJlYXNvbiA9PSAnVENQIFJlc2V0LUknID0+ICdGYWlsZWQnLCBfID0+ICdTdWNjZXNzJykiLCAibnVsbCIsICIxIiwgImNhc2UoJC5yZWFzb24gPT0gJ1RDUCBSZXNldC1JJyA9PiAzLCBfID0+IDApIiwgImFycmF5KCkiLCAicGFyc2VfaW50KCQuYnl0ZXMpIiwgIidPdGhlciciLCAiJzEuMC4wJyIsICIwIiwgIiQucHJvZ3JhbSIsICJwYXJzZV9pbnQoJC5sZXZlbCkiLCAiJ0Npc2NvIEFTQSciLCAiJ0FTQSciLCAiJC5kZXN0X2ludGVyZmFjZSIsICJhcnJheSgwKSIsICIkLmRlc3RfaXAiLCAicGFyc2VfaW50KCQuZGVzdF9wb3J0KSIsICIkLmhvc3RuYW1lIiwgIjQwMDEiLCAiOTkiLCAiJC5yZWFzb24iLCAiJC5tZXNzYWdlX251bWJlciIsICInQ2lzY28nIiwgIiQuY29ubmVjdGlvbl9pZCJd",
          /* input event:
          {
          "age": 28
          }
           * */
          "-i",
          "eyJyZWFzb24iOiAiVENQIFJlc2V0LUkiLCAibGV2ZWwiOiAiNiIsICJkZXN0X2ludGVyZmFjZSI6ICJpbnNpZGUiLCAibWVzc2FnZV9udW1iZXIiOiAiMzAyMDE0IiwgIl9fcmF3X18iOiAiT2N0IDEwIDIwMTggMTI6MzQ6NTYgbG9jYWxob3N0IENpc2NvQVNBWzk5OV06ICVBU0EtNi0zMDIwMTQ6IFRlYXJkb3duIFRDUCBjb25uZWN0aW9uIDExNzQ5IGZvciBvdXRzaWRlOjEwMC42Ni4yMTEuMjQyLzgwIHRvIGluc2lkZToxNzIuMzEuOTguNDQvMTc1OCBkdXJhdGlvbiAwOjAxOjA3IGJ5dGVzIDM4MTEwIFRDUCBSZXNldC1JIiwgInBpZCI6ICI5OTkiLCAicHJvZ3JhbSI6ICJDaXNjb0FTQSIsICJzb3VyY2VfaXAiOiAiMTAwLjY2LjIxMS4yNDIiLCAiZHVyYXRpb24iOiAiMDowMTowNyIsICJob3N0bmFtZSI6ICJsb2NhbGhvc3QiLCAicHJvdG9jb2wiOiAiVENQIiwgImNvbm5lY3Rpb25faWQiOiAiMTE3NDkiLCAic291cmNlX2ludGVyZmFjZSI6ICJvdXRzaWRlIiwgImJ5dGVzIjogIjM4MTEwIiwgInNvdXJjZV9wb3J0IjogIjgwIiwgImRlc3RfaXAiOiAiMTcyLjMxLjk4LjQ0IiwgImRlc3RfcG9ydCI6ICIxNzU4IiwgInRpbWVzdGFtcCI6ICJPY3QgMTAgMjAxOCAxMjozNDo1NiJ9"
        };
    Main.main(args);
  }
}
