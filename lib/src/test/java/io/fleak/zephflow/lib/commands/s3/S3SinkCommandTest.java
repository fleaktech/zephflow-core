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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.dlq.S3DlqWriterTest;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

@Testcontainers
public class S3SinkCommandTest {

  private static final String REGION_STR = "us-east-1";
  private static final String BUCKET_NAME = "fleakdev";

  @Container
  protected static MinIOContainer minioContainer =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withCommand("server /data");

  private S3Client s3Client;

  @BeforeEach
  public void setup() {
    System.setProperty("aws.accessKeyId", minioContainer.getUserName());
    System.setProperty("aws.secretAccessKey", minioContainer.getPassword());
    s3Client = new AwsClientFactory().createS3Client(REGION_STR, null, minioContainer.getS3URL());
    s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
  }

  @AfterEach
  public void teardown() {
    S3DlqWriterTest.deleteAllObjectsInBucket(s3Client, BUCKET_NAME);
    s3Client.deleteBucket(b -> b.bucket(BUCKET_NAME));
    s3Client.close();
  }

  @Test
  public void testWriteIntoS3() throws Exception {

    EncodingType encodingType = EncodingType.CSV;

    S3SinkDto.Config config =
        S3SinkDto.Config.builder()
            .regionStr(REGION_STR)
            .bucketName(BUCKET_NAME)
            .keyName("rr-test")
            .encodingType(encodingType.toString())
            .s3EndpointOverride(minioContainer.getS3URL())
            .build();

    JobContext jobContext = JobContext.builder().metricTags(JOB_CONTEXT.getMetricTags()).build();

    S3SinkCommand command =
        (S3SinkCommand) new S3SinkCommandFactory().createCommand("myNodeId", jobContext);
    command.parseAndValidateArg(toJsonString(config));

    List<RecordFleakData> inputEvents =
        List.of(
            ((RecordFleakData)
                Objects.requireNonNull(FleakData.wrap(Map.of("key1", "101", "key2", "a string")))),
            ((RecordFleakData)
                Objects.requireNonNull(
                    FleakData.wrap(Map.of("key1", "102", "key2", "another string")))));
    ScalarSinkCommand.SinkResult sinkResult;
    try {
      sinkResult =
          command.writeToSink(
              inputEvents, "test_user", new MetricClientProvider.NoopMetricClientProvider());
    } finally {
      command.terminate();
    }

    assertEquals(2, sinkResult.getInputCount());
    assertEquals(2, sinkResult.getSuccessCount());

    // verify s3 content
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    var resp = s3Client.listObjectsV2(listObjectsV2Request);
    assertEquals(1, resp.contents().size());

    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(resp.contents().getFirst().key())
            .build();

    ResponseBytes<GetObjectResponse> s3ObjectBytes = s3Client.getObjectAsBytes(getObjectRequest);
    byte[] data = s3ObjectBytes.asByteArray();
    var deser = DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();
    var actual = deser.deserialize(new SerializedEvent(null, data, null));
    assertEquals(inputEvents, actual);
  }
}
