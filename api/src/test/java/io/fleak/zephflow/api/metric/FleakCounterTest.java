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
package io.fleak.zephflow.api.metric;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Map;
import org.junit.jupiter.api.Test;

class FleakCounterTest {

  @Test
  void recordErrorSampleDefaultsToNoOp() {
    FleakCounter counter =
        new FleakCounter() {
          @Override
          public void increase(Map<String, String> additionalTags) {
            // counting is irrelevant here; only the default recordErrorSample is under test
          }

          @Override
          public void increase(long n, Map<String, String> additionalTags) {
            // counting is irrelevant here; only the default recordErrorSample is under test
          }
        };

    assertDoesNotThrow(() -> counter.recordErrorSample("boom"));
  }
}
