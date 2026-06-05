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
package io.fleak.zephflow.lib.commands.fssource.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SourceIdHasherTest {

  @Test
  void stableAcrossCalls() {
    String a = SourceIdHasher.compute("s3", "s3://bkt/data/", "invoice_(?<ts>\\d+)\\.json");
    String b = SourceIdHasher.compute("s3", "s3://bkt/data/", "invoice_(?<ts>\\d+)\\.json");
    assertEquals(a, b);
  }

  @Test
  void differentRootsDiffer() {
    String a = SourceIdHasher.compute("s3", "s3://bkt/data1/", null);
    String b = SourceIdHasher.compute("s3", "s3://bkt/data2/", null);
    assertNotEquals(a, b);
  }

  @Test
  void length16Hex() {
    String a = SourceIdHasher.compute("file", "/tmp/x", null);
    assertEquals(16, a.length());
    assertTrue(a.matches("[0-9a-f]{16}"));
  }

  @Test
  void nullRegexAllowed() {
    assertDoesNotThrow(() -> SourceIdHasher.compute("file", "/tmp/x", null));
  }
}
