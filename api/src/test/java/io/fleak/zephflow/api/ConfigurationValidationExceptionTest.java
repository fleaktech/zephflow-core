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
package io.fleak.zephflow.api;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConfigurationValidationExceptionTest {
  @Test
  void preservesOriginalFailureWhenADelegatedIssueGetsItsInputLocation() {
    var original = new ClassCastException("original type failure");
    var failure =
        ConfigurationValidationException.at(
                ConfigurationPath.of("sampleRate"),
                original,
                "Must be an integer greater than zero")
            .withPrefix(ConfigurationPath.of("rules").index(1));

    assertSame(original, failure.originalFailure());
    assertSame(original, failure.getCause());
    assertEquals(original.getMessage(), failure.getMessage());
    assertEquals(
        ConfigurationPath.of("rules").index(1).field("sampleRate"),
        failure.issues().getFirst().path());
  }

  @Test
  void keepsLiteralMapKeysDistinctFromIndexesAndNestedProperties() {
    assertNotEquals(ConfigurationPath.of("rules", "0"), ConfigurationPath.of("rules").index(0));
    assertNotEquals(ConfigurationPath.of("a.b"), ConfigurationPath.of("a", "b"));
    var segments = new ArrayList<ConfigurationPath.Segment>();
    segments.add(new ConfigurationPath.Field("\"[]"));
    var path = new ConfigurationPath(segments);
    segments.clear();
    assertEquals(List.of(new ConfigurationPath.Field("\"[]")), path.segments());
    assertThrows(UnsupportedOperationException.class, () -> path.segments().clear());
  }
}
