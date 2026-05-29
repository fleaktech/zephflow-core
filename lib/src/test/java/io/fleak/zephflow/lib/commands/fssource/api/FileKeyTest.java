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
package io.fleak.zephflow.lib.commands.fssource.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class FileKeyTest {

  @Test
  void of_parsesScheme() {
    FileKey k = FileKey.of("s3://bucket/path/file.json");
    assertEquals("s3", k.backend());
    assertEquals("s3://bucket/path/file.json", k.urn());
  }

  @Test
  void of_localFileScheme() {
    FileKey k = FileKey.of("file:///abs/path/x.csv");
    assertEquals("file", k.backend());
  }

  @Test
  void of_gcsScheme() {
    FileKey k = FileKey.of("gs://bucket/k");
    assertEquals("gs", k.backend());
  }

  @Test
  void of_rejectsMissingScheme() {
    assertThrows(IllegalArgumentException.class, () -> FileKey.of("/abs/path"));
  }

  @Test
  void of_rejectsNull() {
    assertThrows(NullPointerException.class, () -> FileKey.of(null));
  }
}
