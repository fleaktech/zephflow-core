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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class S3DlqWriterKeyPrefixTest {

  private static final String BUCKET_NAME = "test-bucket";
  private static final long FIXED_TIMESTAMP = 1700000000000L;

  private S3DlqWriter createWriter(String keyPrefix) {
    return new S3DlqWriter(null, BUCKET_NAME, 1, 1000, keyPrefix);
  }

  @Test
  void testCleanPrefix() {
    S3DlqWriter writer = createWriter("env/pipeline/deployment");
    String key = writer.generateS3ObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("env/pipeline/deployment/dead-letters/"));
    assertFalse(key.contains("//"));
  }

  @Test
  void testLeadingAndTrailingSlashes() {
    S3DlqWriter writer = createWriter("/leading/slash/");
    String key = writer.generateS3ObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("leading/slash/dead-letters/"));
    assertFalse(key.contains("//"));
  }

  @Test
  void testWhitespaceOnlyPrefix() {
    S3DlqWriter writer = createWriter("   ");
    String key = writer.generateS3ObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("dead-letters/"));
  }

  @Test
  void testAllSlashesPrefix() {
    S3DlqWriter writer = createWriter("////");
    String key = writer.generateS3ObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("dead-letters/"));
  }

  @Test
  void testNullPrefix() {
    S3DlqWriter writer = createWriter(null);
    String key = writer.generateS3ObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("dead-letters/"));
  }

  @Test
  void testPrefixWithSurroundingWhitespaceAndSlashes() {
    S3DlqWriter writer = createWriter("  /prod/data/  ");
    String key = writer.generateS3ObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("prod/data/dead-letters/"));
    assertFalse(key.contains("//"));
  }
}
