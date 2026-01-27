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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

@Testcontainers
public class DeltaLakeS3IntegrationTest {

  private static final String REGION_STR = "us-east-1";
  private static final String BUCKET_NAME = "delta-lake-test";

  private static final Schema TEST_SCHEMA =
      new Schema.Parser()
          .parse(
              """
              {"type":"record","name":"TestRecord","fields":[
                {"name":"id","type":"int"},
                {"name":"name","type":"string"},
                {"name":"department","type":"string"},
                {"name":"_fleak_timestamp","type":["null","long"],"default":null}
              ]}""");

  private static final List<Map<String, Object>> TEST_EVENTS =
      List.of(
          Map.of("id", 1, "name", "Alice", "department", "Engineering"),
          Map.of("id", 2, "name", "Bob", "department", "Marketing"),
          Map.of("id", 3, "name", "Charlie", "department", "Engineering"));

  @Container
  protected static MinIOContainer minioContainer =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withCommand("server /data");

  private S3Client s3Client;
  private InputStream inputStream;

  @BeforeEach
  public void setup() {
    inputStream =
        new ByteArrayInputStream(Objects.requireNonNull(toJsonString(TEST_EVENTS)).getBytes());

    System.setProperty("aws.accessKeyId", minioContainer.getUserName());
    System.setProperty("aws.secretAccessKey", minioContainer.getPassword());
    s3Client = new AwsClientFactory().createS3Client(REGION_STR, null, minioContainer.getS3URL());
    s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
  }

  @Test
  public void testDeltalakeSinkWithNativeAvroSchema(@TempDir Path tempDir) throws Exception {
    var tmpFile = tempDir.resolve("test_input.json");
    FileUtils.copyInputStreamToFile(inputStream, tmpFile.toFile());

    String tablePath = "s3a://" + BUCKET_NAME + "/delta-table-" + System.currentTimeMillis();

    UsernamePasswordCredential credential =
        new UsernamePasswordCredential(minioContainer.getUserName(), minioContainer.getPassword());

    createDeltaTable(tablePath, credential);

    Map<String, String> hadoopConfig =
        Map.of("fs.s3a.endpoint", minioContainer.getS3URL(), "fs.s3a.path.style.access", "true");

    ZephFlow flow =
        ZephFlow.startFlow()
            .fileSource(tmpFile.toString(), EncodingType.JSON_ARRAY)
            .deltalakeSink(tablePath, TEST_SCHEMA, null, hadoopConfig, credential);

    flow.execute("test-job", "test-env", "test-service");

    ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    var resp = s3Client.listObjectsV2(listRequest);
    assertFalse(resp.contents().isEmpty(), "Delta table should have written files to S3");

    long parquetFileCount =
        resp.contents().stream().filter(obj -> obj.key().endsWith(".parquet")).count();
    assertTrue(parquetFileCount > 0, "Should have written parquet data files to S3");
  }

  @Test
  public void testDeltalakeSinkMinimalOverload(@TempDir Path tempDir) throws Exception {
    var tmpFile = tempDir.resolve("test_input_minimal.json");
    FileUtils.copyInputStreamToFile(inputStream, tmpFile.toFile());

    String tablePath = tempDir.resolve("delta-table-local").toString();

    createLocalDeltaTable(tablePath);

    ZephFlow flow =
        ZephFlow.startFlow()
            .fileSource(tmpFile.toString(), EncodingType.JSON_ARRAY)
            .deltalakeSink(tablePath, TEST_SCHEMA);

    flow.execute("test-job-minimal", "test-env", "test-service");

    long parquetFileCount = countParquetFiles(Path.of(tablePath));
    assertTrue(parquetFileCount > 0, "Should have written parquet data files to local Delta table");

    long recordCount = getRecordCountFromDeltaLog(Path.of(tablePath));
    assertEquals(3, recordCount, "Should have written exactly 3 records");
  }

  @Test
  @Disabled(
      "Delta Kernel 4.0.0 does not support partitioned writes - "
          + "ColumnarBatch.withDeletedColumnAt() throws UnsupportedOperationException")
  public void testDeltalakeSinkWithPartitionColumns(@TempDir Path tempDir) throws Exception {
    var tmpFile = tempDir.resolve("test_input_partition.json");
    FileUtils.copyInputStreamToFile(inputStream, tmpFile.toFile());

    String tablePath = tempDir.resolve("delta-table-partitioned").toString();

    createLocalDeltaTableWithPartition(tablePath);

    ZephFlow flow =
        ZephFlow.startFlow()
            .fileSource(tmpFile.toString(), EncodingType.JSON_ARRAY)
            .deltalakeSink(tablePath, TEST_SCHEMA, List.of("department"));

    flow.execute("test-job-partitioned", "test-env", "test-service");

    File deltaLog = Path.of(tablePath, "_delta_log").toFile();
    assertTrue(deltaLog.exists(), "Delta log should exist");
  }

  @Test
  public void testDeltalakeSink_EmptyInput(@TempDir Path tempDir) throws Exception {
    var tmpFile = tempDir.resolve("empty_input.json");
    Files.writeString(tmpFile, "[]");

    String tablePath = tempDir.resolve("delta-table-empty").toString();

    createLocalDeltaTable(tablePath);

    ZephFlow flow =
        ZephFlow.startFlow()
            .fileSource(tmpFile.toString(), EncodingType.JSON_ARRAY)
            .deltalakeSink(tablePath, TEST_SCHEMA);

    flow.execute("test-job-empty", "test-env", "test-service");

    long recordCount = getRecordCountFromDeltaLog(Path.of(tablePath));
    assertEquals(0, recordCount, "Should have written 0 records for empty input");
  }

  private void createDeltaTable(String tablePath, UsernamePasswordCredential credential) {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.access.key", credential.getUsername());
    hadoopConf.set("fs.s3a.secret.key", credential.getPassword());
    hadoopConf.set("fs.s3a.endpoint", minioContainer.getS3URL());
    hadoopConf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");

    createDeltaTableWithConfig(tablePath, hadoopConf);
  }

  private void createLocalDeltaTable(String tablePath) {
    Configuration hadoopConf = new Configuration();
    createDeltaTableWithConfig(tablePath, hadoopConf);
  }

  private void createLocalDeltaTableWithPartition(String tablePath) {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);

    StructType schema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("name", StringType.STRING, true),
                new StructField("department", StringType.STRING, true),
                new StructField("_fleak_timestamp", LongType.LONG, true)));

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(
            engine, "Create partitioned test table", Operation.CREATE_TABLE);

    txnBuilder = txnBuilder.withSchema(engine, schema);
    txnBuilder = txnBuilder.withPartitionColumns(engine, List.of("department"));

    Transaction txn = txnBuilder.build(engine);
    CloseableIterable<io.delta.kernel.data.Row> emptyActions = new EmptyCloseableIterable<>();
    txn.commit(engine, emptyActions);
  }

  private void createDeltaTableWithConfig(String tablePath, Configuration hadoopConf) {
    Engine engine = DefaultEngine.create(hadoopConf);

    StructType schema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("name", StringType.STRING, true),
                new StructField("department", StringType.STRING, true),
                new StructField("_fleak_timestamp", LongType.LONG, true)));

    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "Create test table", Operation.CREATE_TABLE);

    txnBuilder = txnBuilder.withSchema(engine, schema);

    Transaction txn = txnBuilder.build(engine);
    CloseableIterable<io.delta.kernel.data.Row> emptyActions = new EmptyCloseableIterable<>();
    txn.commit(engine, emptyActions);
  }

  private long countParquetFiles(Path tablePath) throws Exception {
    return Files.walk(tablePath).filter(p -> p.toString().endsWith(".parquet")).count();
  }

  private long getRecordCountFromDeltaLog(Path tablePath) throws Exception {
    Path deltaLogPath = tablePath.resolve("_delta_log");
    long totalRecords = 0;
    try (var files = Files.list(deltaLogPath)) {
      for (Path commitFile :
          files
              .filter(p -> p.toString().endsWith(".json"))
              .filter(p -> !p.getFileName().toString().startsWith("_"))
              .toList()) {
        String content = Files.readString(commitFile);
        for (String line : content.split("\n")) {
          if (line.contains("\"add\"")) {
            JsonNode node = OBJECT_MAPPER.readTree(line);
            JsonNode addNode = node.get("add");
            if (addNode != null && addNode.has("stats")) {
              JsonNode statsNode = OBJECT_MAPPER.readTree(addNode.get("stats").asText());
              if (statsNode.has("numRecords")) {
                totalRecords += statsNode.get("numRecords").asLong();
              }
            }
          }
        }
      }
    }
    return totalRecords;
  }

  @AfterEach
  public void teardown() {
    S3IntegrationTest.deleteAllObjectsInBucket(s3Client, BUCKET_NAME);
    s3Client.deleteBucket(b -> b.bucket(BUCKET_NAME));
    s3Client.close();
  }

  private static class EmptyCloseableIterable<T> implements CloseableIterable<T> {
    @Override
    public io.delta.kernel.utils.@NotNull CloseableIterator<T> iterator() {
      return new EmptyCloseableIterator<>();
    }

    @Override
    public void close() {}
  }

  private static class EmptyCloseableIterator<T>
      implements io.delta.kernel.utils.CloseableIterator<T> {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public T next() {
      throw new java.util.NoSuchElementException("Empty iterator");
    }

    @Override
    public void close() {}
  }
}
