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
  void theSameBucketAlwaysGetsTheSameId() {
    String first = SourceIdHasher.compute("scope", "n", "s3", "s3://b/r", null, null, 7);
    String second = SourceIdHasher.compute("scope", "n", "s3", "s3://b/r", null, null, 7);

    assertEquals(first, second);
  }

  @Test
  void differentBucketsGetDifferentIds() {
    String seven = SourceIdHasher.compute("scope", "n", "s3", "s3://b/r", null, null, 7);
    String eight = SourceIdHasher.compute("scope", "n", "s3", "s3://b/r", null, null, 8);

    assertNotEquals(seven, eight);
  }

  @Test
  void theIdDoesNotDependOnTheReplicaLayout() {
    // A bucket keeps its id whether it is owned by replica 1 of 3 or replica 3 of 7.
    assertEquals(
        SourceIdHasher.compute("scope", "n", "s3", "s3://b/r", null, null, 10),
        SourceIdHasher.compute("scope", "n", "s3", "s3://b/r", null, null, 10));
  }

  @Test
  void differentScopesGetDifferentIds() {
    assertNotEquals(
        SourceIdHasher.compute("scope-a", "n", "s3", "s3://b/r", null, null, 1),
        SourceIdHasher.compute("scope-b", "n", "s3", "s3://b/r", null, null, 1));
  }
}
