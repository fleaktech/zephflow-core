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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import static org.junit.jupiter.api.Assertions.*;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.fleak.zephflow.lib.azure.AzureClientFactory;
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
class AzureBackendIntegrationTest {

  private static final int AZURITE_BLOB_PORT = 10000;
  private static final String AZURITE_ACCOUNT = "devstoreaccount1";
  // Valid 64-byte base64 key used only for testing (set on the Azurite container via env var)
  private static final String AZURITE_KEY =
      "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==";

  @Container
  static GenericContainer<?> AZURITE =
      new GenericContainer<>(
              DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.33.0"))
          .withExposedPorts(AZURITE_BLOB_PORT)
          .withCommand("azurite-blob", "--blobHost", "0.0.0.0")
          .withEnv("AZURITE_ACCOUNTS", AZURITE_ACCOUNT + ":" + AZURITE_KEY);

  private static String connectionString() {
    return "DefaultEndpointsProtocol=http;AccountName="
        + AZURITE_ACCOUNT
        + ";AccountKey="
        + AZURITE_KEY
        + ";BlobEndpoint=http://"
        + AZURITE.getHost()
        + ":"
        + AZURITE.getMappedPort(AZURITE_BLOB_PORT)
        + "/"
        + AZURITE_ACCOUNT
        + ";";
  }

  private static BlobServiceClient serviceClient() {
    return new AzureClientFactory().createBlobServiceClientFromConnectionString(connectionString());
  }

  @BeforeEach
  void setup() {
    BlobServiceClient svc = serviceClient();
    BlobContainerClient container = svc.createBlobContainerIfNotExists("test-bkt");
    container
        .getBlobClient("data/evt_1.log")
        .upload(
            new java.io.ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)), 5, true);
    container
        .getBlobClient("data/evt_2.log")
        .upload(
            new java.io.ByteArrayInputStream("world".getBytes(StandardCharsets.UTF_8)), 5, true);
    container
        .getBlobClient("data/skip.txt")
        .upload(new java.io.ByteArrayInputStream("nope".getBytes(StandardCharsets.UTF_8)), 4, true);
  }

  @AfterEach
  void teardown() {
    serviceClient().deleteBlobContainerIfExists("test-bkt");
  }

  private static String rootUrn() {
    return "https://" + AZURITE_ACCOUNT + ".blob.core.windows.net/test-bkt/data/";
  }

  @Test
  void listsMatchingBlobs() {
    AzureBackendConfig cfg = new AzureBackendConfig(connectionString(), null, null);
    AzureBackend backend = new AzureBackend();
    FileLister lister = backend.createLister(cfg);

    List<FileEntry> entries =
        lister.list(new ListRequest(rootUrn(), Pattern.compile("evt_\\d+\\.log"))).toList();

    assertEquals(2, entries.size());
    assertTrue(entries.stream().allMatch(e -> e.key().backend().equals("azblob")));
    assertTrue(entries.stream().allMatch(e -> e.key().urn().startsWith("https://")));
  }

  @Test
  void listsAndReadsBlob() throws Exception {
    AzureBackendConfig cfg = new AzureBackendConfig(connectionString(), null, null);
    AzureBackend backend = new AzureBackend();
    FileLister lister = backend.createLister(cfg);
    FileReader reader = backend.createReader(cfg);

    List<FileEntry> entries =
        lister.list(new ListRequest(rootUrn(), Pattern.compile("evt_\\d+\\.log"))).toList();
    assertEquals(2, entries.size());

    try (InputStream in = reader.open(entries.get(0).key(), 0)) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(body.equals("hello") || body.equals("world"));
    }
  }

  @Test
  void readsWithOffset() throws Exception {
    AzureBackendConfig cfg = new AzureBackendConfig(connectionString(), null, null);
    AzureBackend backend = new AzureBackend();
    FileLister lister = backend.createLister(cfg);
    FileReader reader = backend.createReader(cfg);

    List<FileEntry> all = lister.list(new ListRequest(rootUrn(), null)).toList();
    FileEntry evt1 =
        all.stream().filter(e -> e.displayPath().endsWith("evt_1.log")).findFirst().orElseThrow();

    try (InputStream in = reader.open(evt1.key(), 2)) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("llo", body);
    }
  }

  @Test
  void schemeIsAzblob() {
    assertEquals("azblob", new AzureBackend().scheme());
  }
}
