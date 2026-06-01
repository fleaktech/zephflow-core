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
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@Testcontainers
class GcsBackendIntegrationTest {

  @Container
  static GenericContainer<?> FAKE_GCS =
      new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:1.49"))
          .withExposedPorts(4443)
          .withCommand("-scheme", "http");

  private static Storage storage() {
    return StorageOptions.newBuilder()
        .setHost("http://" + FAKE_GCS.getHost() + ":" + FAKE_GCS.getMappedPort(4443))
        .setProjectId("test-proj")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  @BeforeEach
  void setupBucket() {
    Storage s = storage();
    s.create(BucketInfo.of("test-bkt"));
    s.create(
        BlobInfo.newBuilder("test-bkt", "data/evt_1.log").build(),
        "hello".getBytes(StandardCharsets.UTF_8));
    s.create(
        BlobInfo.newBuilder("test-bkt", "data/evt_2.log").build(),
        "world".getBytes(StandardCharsets.UTF_8));
    s.create(
        BlobInfo.newBuilder("test-bkt", "data/skip.txt").build(),
        "nope".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void listsAndReads() throws Exception {
    Storage s = storage();
    GcsLister lister = new GcsLister(s);
    GcsReader reader = new GcsReader(s);

    List<FileEntry> entries =
        lister
            .list(new ListRequest("gs://test-bkt/data/", Pattern.compile("evt_\\d+\\.log")))
            .toList();
    assertEquals(2, entries.size());

    try (InputStream in = reader.open(entries.get(0).key(), 0)) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(body.equals("hello") || body.equals("world"));
    }
  }
}
