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

  private static final String SCOPE = "11111111-1111-4111-8111-111111111111";
  private static final String OTHER_SCOPE = "22222222-2222-4222-8222-222222222222";
  private static final String NODE_ID = "s3_source";
  private static final String ROOT = "s3://bkt/data/";
  private static final String FILE_NAME_REGEX = "invoice_(?<ts>\\d+)\\.json";

  private static String computeForRoot(String checkpointScope, String nodeId, String root) {
    return SourceIdHasher.compute(checkpointScope, nodeId, "s3", root, FILE_NAME_REGEX, null, 0, 1);
  }

  @Test
  void stableAcrossCalls() {
    assertEquals(computeForRoot(SCOPE, NODE_ID, ROOT), computeForRoot(SCOPE, NODE_ID, ROOT));
  }

  @Test
  void differentCheckpointScopesDiffer() {
    assertNotEquals(
        computeForRoot(SCOPE, NODE_ID, ROOT), computeForRoot(OTHER_SCOPE, NODE_ID, ROOT));
  }

  @Test
  void differentNodeIdsDiffer() {
    assertNotEquals(
        computeForRoot(SCOPE, "first_source", ROOT), computeForRoot(SCOPE, "second_source", ROOT));
  }

  @Test
  void differentRootsDiffer() {
    assertNotEquals(
        computeForRoot(SCOPE, NODE_ID, "s3://bkt/data1/"),
        computeForRoot(SCOPE, NODE_ID, "s3://bkt/data2/"));
  }

  @Test
  void length16Hex() {
    String id = computeForRoot(SCOPE, NODE_ID, ROOT);
    assertEquals(16, id.length());
    assertTrue(id.matches("[0-9a-f]{16}"));
  }

  @Test
  void nullRegexAllowed() {
    assertDoesNotThrow(
        () -> SourceIdHasher.compute(SCOPE, NODE_ID, "file", "/tmp/x", null, null, 0, 1));
  }

  @Test
  void distinctIdsPerReplica() {
    String replica0 = SourceIdHasher.compute(SCOPE, NODE_ID, "s3", ROOT, null, null, 0, 3);
    String replica1 = SourceIdHasher.compute(SCOPE, NODE_ID, "s3", ROOT, null, null, 1, 3);
    String replica2 = SourceIdHasher.compute(SCOPE, NODE_ID, "s3", ROOT, null, null, 2, 3);

    assertNotEquals(replica0, replica1);
    assertNotEquals(replica1, replica2);
    assertNotEquals(replica0, replica2);
  }

  @Test
  void exactObjectKeyChangesCheckpointIdentity() {
    String first =
        SourceIdHasher.compute(SCOPE, NODE_ID, "s3", ROOT, null, "root/a/events.jsonl", 0, 1);
    String second =
        SourceIdHasher.compute(SCOPE, NODE_ID, "s3", ROOT, null, "root/b/events.jsonl", 0, 1);

    assertNotEquals(first, second);
  }
}
