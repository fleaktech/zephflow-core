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
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
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
import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
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
            .batching(false)
            .build();

    JobContext jobContext = JobContext.builder().metricTags(JOB_CONTEXT.getMetricTags()).build();

    S3SinkCommand command =
        (S3SinkCommand) new S3SinkCommandFactory().createCommand("myNodeId", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    List<RecordFleakData> inputEvents =
        List.of(
            ((RecordFleakData)
                Objects.requireNonNull(FleakData.wrap(Map.of("key1", "101", "key2", "a string")))),
            ((RecordFleakData)
                Objects.requireNonNull(
                    FleakData.wrap(Map.of("key1", "102", "key2", "another string")))));
    ScalarSinkCommand.SinkResult sinkResult;
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var context = command.getExecutionContext();
      sinkResult = command.writeToSink(inputEvents, "test_user", context);
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
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(resp.contents().get(0).key()).build();

    ResponseBytes<GetObjectResponse> s3ObjectBytes = s3Client.getObjectAsBytes(getObjectRequest);
    byte[] data = s3ObjectBytes.asByteArray();
    var deser = DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();
    var actual = deser.deserialize(new SerializedEvent(null, data, null));
    assertEquals(inputEvents, actual);
  }

  @Test
  public void testWriteParquetIntoS3(@TempDir File tempDir) throws Exception {
    S3SinkDto.Config config =
        S3SinkDto.Config.builder()
            .regionStr(REGION_STR)
            .bucketName(BUCKET_NAME)
            .keyName("parquet-test")
            .encodingType(EncodingType.PARQUET.toString())
            .s3EndpointOverride(minioContainer.getS3URL())
            .batching(true)
            .batchSize(2)
            .avroSchema(
                Map.of(
                    "type", "record",
                    "name", "TestRecord",
                    "fields",
                        List.of(
                            Map.of("name", "key1", "type", "string"),
                            Map.of("name", "key2", "type", "string"))))
            .build();

    JobContext jobContext = JobContext.builder().metricTags(JOB_CONTEXT.getMetricTags()).build();

    S3SinkCommand command =
        (S3SinkCommand) new S3SinkCommandFactory().createCommand("myNodeId", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    List<RecordFleakData> inputEvents =
        List.of(
            ((RecordFleakData)
                Objects.requireNonNull(FleakData.wrap(Map.of("key1", "101", "key2", "a string")))),
            ((RecordFleakData)
                Objects.requireNonNull(
                    FleakData.wrap(Map.of("key1", "102", "key2", "another string")))));

    ScalarSinkCommand.SinkResult sinkResult;
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var context = command.getExecutionContext();
      sinkResult = command.writeToSink(inputEvents, "test_user", context);
    } finally {
      command.terminate();
    }

    assertEquals(2, sinkResult.getInputCount());
    assertEquals(2, sinkResult.getSuccessCount());

    // verify S3 object key matches partition pattern
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    var resp = s3Client.listObjectsV2(listObjectsV2Request);
    assertEquals(1, resp.contents().size());

    String objectKey = resp.contents().get(0).key();
    String expectedPattern =
        "parquet-test/year=\\d{4}/month=\\d{2}/day=\\d{2}/[0-9a-f-]+\\.parquet";
    assertTrue(objectKey.matches(expectedPattern), "Key should match pattern: " + objectKey);

    // download parquet file and verify content
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(objectKey).build();
    ResponseBytes<GetObjectResponse> s3ObjectBytes = s3Client.getObjectAsBytes(getObjectRequest);

    File parquetFile = new File(tempDir, "downloaded.parquet");
    try (FileOutputStream fos = new FileOutputStream(parquetFile)) {
      fos.write(s3ObjectBytes.asByteArray());
    }

    StructType schema =
        new StructType(
            List.of(
                new StructField("key1", StringType.STRING, true),
                new StructField("key2", StringType.STRING, true)));

    List<Map<String, Object>> actualRecords = readParquetRecords(parquetFile, schema);

    List<Map<String, Object>> expectedRecords =
        List.of(
            Map.of("key1", "101", "key2", "a string"),
            Map.of("key1", "102", "key2", "another string"));

    assertEquals(expectedRecords, actualRecords);
  }

  @Test
  public void testWriteIntoS3_noDoubleSlashWithTrailingSlashInKeyName() throws Exception {
    EncodingType encodingType = EncodingType.JSON_OBJECT_LINE;
    String keyNameWithTrailingSlash = "my-prefix/";

    S3SinkDto.Config config =
        S3SinkDto.Config.builder()
            .regionStr(REGION_STR)
            .bucketName(BUCKET_NAME)
            .keyName(keyNameWithTrailingSlash)
            .encodingType(encodingType.toString())
            .s3EndpointOverride(minioContainer.getS3URL())
            .batching(true)
            .batchSize(1)
            .build();

    JobContext jobContext = JobContext.builder().metricTags(JOB_CONTEXT.getMetricTags()).build();

    S3SinkCommand command =
        (S3SinkCommand) new S3SinkCommandFactory().createCommand("myNodeId", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    List<RecordFleakData> inputEvents =
        List.of(
            ((RecordFleakData)
                Objects.requireNonNull(FleakData.wrap(Map.of("key1", "101", "key2", "value")))));

    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var context = command.getExecutionContext();
      command.writeToSink(inputEvents, "test_user", context);
    } finally {
      command.terminate();
    }

    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    var resp = s3Client.listObjectsV2(listObjectsV2Request);
    assertEquals(1, resp.contents().size());

    String objectKey = resp.contents().get(0).key();
    assertFalse(objectKey.contains("//"), "S3 key should not contain double slash: " + objectKey);
    String expectedPattern = "my-prefix/year=\\d{4}/month=\\d{2}/day=\\d{2}/[0-9a-f-]+\\.jsonl";
    assertTrue(objectKey.matches(expectedPattern), "Key should match pattern: " + objectKey);
  }

  private List<Map<String, Object>> readParquetRecords(File parquetFile, StructType schema)
      throws Exception {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);

    String filePath = parquetFile.toURI().toString();
    FileStatus fileStatus =
        FileStatus.of(filePath, parquetFile.length(), parquetFile.lastModified());

    List<Map<String, Object>> records = new ArrayList<>();
    List<FileStatus> fileStatuses = List.of(fileStatus);

    CloseableIterator<FileStatus> fileStatusIterator =
        new CloseableIterator<>() {
          private final Iterator<FileStatus> iter = fileStatuses.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public FileStatus next() {
            return iter.next();
          }

          @Override
          public void close() {}
        };

    try (CloseableIterator<ColumnarBatch> batches =
        engine.getParquetHandler().readParquetFiles(fileStatusIterator, schema, Optional.empty())) {
      while (batches.hasNext()) {
        ColumnarBatch batch = batches.next();
        int numRows = batch.getSize();
        for (int row = 0; row < numRows; row++) {
          Map<String, Object> record = new LinkedHashMap<>();
          for (int col = 0; col < schema.length(); col++) {
            String fieldName = schema.at(col).getName();
            var columnVector = batch.getColumnVector(col);
            if (!columnVector.isNullAt(row)) {
              record.put(fieldName, columnVector.getString(row));
            }
          }
          records.add(record);
        }
      }
    }
    return records;
  }
}
