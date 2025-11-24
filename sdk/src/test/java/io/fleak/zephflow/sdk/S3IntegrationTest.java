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
package io.fleak.zephflow.sdk;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.sdk.ZephFlowTest.SOURCE_EVENTS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/** Created by bolei on 3/17/25 */
@Testcontainers
public class S3IntegrationTest {

  private static final String REGION_STR = "us-east-1";
  private static final String BUCKET_NAME = "fleakdev";
  private static final String FOLDER_NAME = "test_folder";

  @Container
  protected static MinIOContainer minioContainer =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withCommand("server /data");

  private S3Client s3Client;
  private InputStream in;

  @BeforeEach
  public void setup() {
    in = new ByteArrayInputStream(Objects.requireNonNull(toJsonString(SOURCE_EVENTS)).getBytes());

    System.setProperty("aws.accessKeyId", minioContainer.getUserName());
    System.setProperty("aws.secretAccessKey", minioContainer.getPassword());
    s3Client = new AwsClientFactory().createS3Client(REGION_STR, null, minioContainer.getS3URL());
    s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
  }

  @Test
  public void testS3Sink(@TempDir Path tempDir) throws Exception {
    var tmpFile = tempDir.resolve("test_input.json");
    FileUtils.copyInputStreamToFile(in, tmpFile.toFile());
    ZephFlow flow = ZephFlow.startFlow();
    var inputStream = flow.fileSource(tmpFile.toString(), EncodingType.JSON_ARRAY);
    var outputStream =
        inputStream.s3Sink(
            REGION_STR,
            BUCKET_NAME,
            FOLDER_NAME,
            EncodingType.JSON_OBJECT_LINE,
            null, // No credentials - use default credential chain
            minioContainer.getS3URL());
    outputStream.execute("test_jobid", "test_env", "test_service");

    // verify s3 content
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    var resp = s3Client.listObjectsV2(listObjectsV2Request);
    assertEquals(1, resp.contents().size());

    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(resp.contents().get(0).key()).build();

    ResponseBytes<GetObjectResponse> s3ObjectBytes = s3Client.getObjectAsBytes(getObjectRequest);
    byte[] data = s3ObjectBytes.asByteArray();
    var deser =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT_LINE)
            .createDeserializer();
    var actual = deser.deserialize(new SerializedEvent(null, data, null));
    var actualUnwrapped = actual.stream().map(FleakData::unwrap).toList();
    assertEquals(SOURCE_EVENTS, actualUnwrapped);
  }

  @Test
  public void testS3SinkWithCredentials(@TempDir Path tempDir) throws Exception {
    var tmpFile = tempDir.resolve("test_input_with_credentials.json");
    FileUtils.copyInputStreamToFile(in, tmpFile.toFile());

    // Create test credentials (MinIO uses these defaults)
    var credentials =
        new io.fleak.zephflow.lib.credentials.UsernamePasswordCredential(
            "minioadmin", "minioadmin");

    ZephFlow flow = ZephFlow.startFlow();
    var inputStream = flow.fileSource(tmpFile.toString(), EncodingType.JSON_ARRAY);
    var outputStream =
        inputStream.s3Sink(
            REGION_STR,
            BUCKET_NAME,
            "test-credentials-folder",
            EncodingType.JSON_OBJECT_LINE,
            credentials, // Pass credential object directly
            minioContainer.getS3URL());
    outputStream.execute("test_jobid_with_creds", "test_env", "test_service");

    // verify s3 content
    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder()
            .bucket(BUCKET_NAME)
            .prefix("test-credentials-folder/")
            .build();
    ListObjectsV2Response resp = s3Client.listObjectsV2(listRequest);
    assertEquals(1, resp.contents().size());

    // verify object content
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(resp.contents().get(0).key()).build();

    ResponseBytes<GetObjectResponse> s3ObjectBytes = s3Client.getObjectAsBytes(getObjectRequest);
    byte[] data = s3ObjectBytes.asByteArray();
    var deser =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT_LINE)
            .createDeserializer();
    var actual = deser.deserialize(new SerializedEvent(null, data, null));
    var actualUnwrapped = actual.stream().map(FleakData::unwrap).toList();
    assertEquals(SOURCE_EVENTS, actualUnwrapped);
  }

  @AfterEach
  public void teardown() {

    deleteAllObjectsInBucket(s3Client, BUCKET_NAME);
    s3Client.deleteBucket(b -> b.bucket(BUCKET_NAME));
    s3Client.close();

    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteAllObjectsInBucket(S3Client s3Client, String bucketName) {
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucketName).build();

    ListObjectsV2Response listResponse;
    do {
      listResponse = s3Client.listObjectsV2(listRequest);

      // Delete objects one by one instead of in bulk
      for (S3Object s3Object : listResponse.contents()) {
        DeleteObjectRequest deleteRequest =
            DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();

        s3Client.deleteObject(deleteRequest);
      }

      listRequest =
          listRequest.toBuilder().continuationToken(listResponse.nextContinuationToken()).build();
    } while (listResponse.isTruncated());
  }
}
