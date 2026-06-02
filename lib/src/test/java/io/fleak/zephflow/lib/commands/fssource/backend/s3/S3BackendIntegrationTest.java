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
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Tag("integration")
@Testcontainers
class S3BackendIntegrationTest {

  @Container
  static LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5"))
          .withServices(LocalStackContainer.Service.S3)
          .withStartupTimeout(Duration.ofMinutes(2));

  private static S3Client s3() {
    return S3Client.builder()
        .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .region(Region.of(LOCALSTACK.getRegion()))
        .forcePathStyle(true)
        .build();
  }

  @BeforeEach
  void setupBucket() {
    try (S3Client c = s3()) {
      c.createBucket(CreateBucketRequest.builder().bucket("test-bkt").build());
      c.putObject(
          PutObjectRequest.builder().bucket("test-bkt").key("data/evt_1.log").build(),
          RequestBody.fromString("hello"));
      c.putObject(
          PutObjectRequest.builder().bucket("test-bkt").key("data/evt_2.log").build(),
          RequestBody.fromString("world"));
      c.putObject(
          PutObjectRequest.builder().bucket("test-bkt").key("data/skip.txt").build(),
          RequestBody.fromString("nope"));
    }
  }

  @AfterEach
  void teardownBucket() {
    try (S3Client c = s3()) {
      // Delete all objects first
      try {
        var objects = c.listObjectsV2(ListObjectsV2Request.builder().bucket("test-bkt").build());
        for (var obj : objects.contents()) {
          c.deleteObject(DeleteObjectRequest.builder().bucket("test-bkt").key(obj.key()).build());
        }
        c.deleteBucket(DeleteBucketRequest.builder().bucket("test-bkt").build());
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  void listsAndReadsMatchingObjects() throws Exception {
    S3BackendConfig cfg =
        new S3BackendConfig(
            LOCALSTACK.getRegion(),
            null,
            LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    S3Backend backend = new S3Backend();
    FileLister lister = backend.createLister(cfg);
    FileReader reader = backend.createReader(cfg);

    List<FileEntry> entries =
        lister
            .list(new ListRequest("s3://test-bkt/data/", Pattern.compile("evt_\\d+\\.log")))
            .toList();
    assertEquals(2, entries.size());

    try (InputStream in = reader.open(entries.get(0).key(), 0)) {
      String body = new String(in.readAllBytes());
      assertTrue(body.equals("hello") || body.equals("world"));
    }
  }
}
