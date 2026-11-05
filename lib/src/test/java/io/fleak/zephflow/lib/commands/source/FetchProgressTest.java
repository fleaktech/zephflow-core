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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FetchProgressTest {

  @Test
  void returnsEachRecordOnceInFetchOrder() {
    FetchProgress<String> progress = new FetchProgress<>();
    List<List<String>> returned = new ArrayList<>();

    returned.add(progress.next(List.of("a", "b", "c", "d")));
    returned.add(progress.through(1));
    returned.add(progress.through(1));
    returned.add(progress.through(3));
    returned.add(progress.through(2));
    returned.add(progress.next(List.of("e", "f")));
    returned.add(progress.through(9));
    returned.add(progress.rest());
    returned.add(progress.next(List.of("g")));
    returned.add(progress.rest());
    returned.add(progress.next(List.of()));

    assertEquals(
        List.of(
            List.of(),
            List.of("a"),
            List.of(),
            List.of("b", "c"),
            List.of(),
            List.of("d"),
            List.of("e", "f"),
            List.of(),
            List.of(),
            List.of("g"),
            List.of()),
        returned);
  }
}
