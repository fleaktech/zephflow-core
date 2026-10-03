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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.S3DlqWriterTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

@Testcontainers
class DeltaLakeSinkMinioIntegrationTest {

  private static final String REGION_STR = "us-east-1";
  private static final String BUCKET_NAME = "delta-lake-test";
  private static final String CREDENTIAL_ID = "minio_credential";

  private static final Map<String, Object> AVRO_SCHEMA =
      fromJsonString(
          """
          {"type":"record","name":"TestRecord","fields":[
            {"name":"id","type":"int"},
            {"name":"name","type":"string"},
            {"name":"department","type":"string"},
            {"name":"_fleak_timestamp","type":["null","long"],"default":null}
          ]}""",
          new TypeReference<>() {});

  private static final StructType TABLE_SCHEMA =
      new StructType(
          List.of(
              new StructField("id", IntegerType.INTEGER, false),
              new StructField("name", StringType.STRING, true),
              new StructField("department", StringType.STRING, true),
              new StructField("_fleak_timestamp", LongType.LONG, true)));

  private static final List<Map<String, Object>> TEST_EVENTS =
      List.of(
          Map.of("id", 1, "name", "Alice", "department", "Engineering"),
          Map.of("id", 2, "name", "Bob", "department", "Marketing"),
          Map.of("id", 3, "name", "Charlie", "department", "Engineering"));

  @Container
  private static final MinIOContainer MINIO_CONTAINER =
      new MinIOContainer(TestUtils.MINIO_IMAGE).withCommand("server /data");

  private S3Client s3Client;

  @BeforeEach
  void setup() {
    System.setProperty("aws.accessKeyId", MINIO_CONTAINER.getUserName());
    System.setProperty("aws.secretAccessKey", MINIO_CONTAINER.getPassword());
    s3Client = new AwsClientFactory().createS3Client(REGION_STR, null, MINIO_CONTAINER.getS3URL());
    s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
  }

  @AfterEach
  void teardown() {
    S3DlqWriterTest.deleteAllObjectsInBucket(s3Client, BUCKET_NAME);
    s3Client.deleteBucket(b -> b.bucket(BUCKET_NAME));
    s3Client.close();
  }

  @Test
  void testDeltaLakeSinkWithNativeAvroSchemaOnMinio() throws Exception {
    String tableKey = "delta-table-" + System.currentTimeMillis();
    String tablePath = "s3a://" + BUCKET_NAME + "/" + tableKey;
    Configuration s3aConf = s3aConfiguration();
    createDeltaTable(s3aConf, tablePath, List.of());

    DeltaLakeSinkDto.Config config =
        DeltaLakeSinkDto.Config.builder()
            .tablePath(tablePath)
            .avroSchema(AVRO_SCHEMA)
            .hadoopConfiguration(
                Map.of(
                    "fs.s3a.endpoint",
                    MINIO_CONTAINER.getS3URL(),
                    "fs.s3a.path.style.access",
                    "true"))
            .credentialId(CREDENTIAL_ID)
            .build();
    JobContext jobContext =
        TestUtils.buildJobContext(
            new HashMap<>(
                Map.of(
                    CREDENTIAL_ID,
                    new UsernamePasswordCredential(
                        MINIO_CONTAINER.getUserName(), MINIO_CONTAINER.getPassword()))));

    assertEquals(
        new ScalarSinkCommand.SinkResult(3, 0, List.of()),
        writeEvents(config, jobContext, TEST_EVENTS));

    assertEquals(
        new TableContent(3, TEST_EVENTS),
        readTableContent(s3aConf, tablePath, s3DeltaLogCommits(tableKey)));
  }

  @Test
  void testDeltaLakeSinkWithMinimalConfig(@TempDir Path tempDir) throws Exception {
    Path tableDir = tempDir.resolve("delta-table-local");
    createDeltaTable(new Configuration(), tableDir.toString(), List.of());

    assertEquals(
        new ScalarSinkCommand.SinkResult(3, 0, List.of()),
        writeEvents(minimalConfig(tableDir), TestUtils.JOB_CONTEXT, TEST_EVENTS));

    assertEquals(
        new TableContent(3, TEST_EVENTS),
        readTableContent(new Configuration(), tableDir.toString(), localDeltaLogCommits(tableDir)));
  }

  @Test
  @Disabled(
      "Delta Kernel 4.0.0 does not support partitioned writes - "
          + "ColumnarBatch.withDeletedColumnAt() throws UnsupportedOperationException")
  void testDeltaLakeSinkWithPartitionColumns(@TempDir Path tempDir) throws Exception {
    Path tableDir = tempDir.resolve("delta-table-partitioned");
    createDeltaTable(new Configuration(), tableDir.toString(), List.of("department"));

    DeltaLakeSinkDto.Config config =
        DeltaLakeSinkDto.Config.builder()
            .tablePath(tableDir.toString())
            .avroSchema(AVRO_SCHEMA)
            .partitionColumns(List.of("department"))
            .build();
    assertEquals(
        new ScalarSinkCommand.SinkResult(3, 0, List.of()),
        writeEvents(config, TestUtils.JOB_CONTEXT, TEST_EVENTS));

    assertEquals(
        new TableContent(3, TEST_EVENTS),
        readTableContent(new Configuration(), tableDir.toString(), localDeltaLogCommits(tableDir)));
  }

  @Test
  void testDeltaLakeSinkWithEmptyInput(@TempDir Path tempDir) throws Exception {
    Path tableDir = tempDir.resolve("delta-table-empty");
    createDeltaTable(new Configuration(), tableDir.toString(), List.of());

    assertEquals(
        new ScalarSinkCommand.SinkResult(0, 0, List.of()),
        writeEvents(minimalConfig(tableDir), TestUtils.JOB_CONTEXT, List.of()));

    assertEquals(
        new TableContent(0, List.of()),
        readTableContent(new Configuration(), tableDir.toString(), localDeltaLogCommits(tableDir)));
  }

  private record TableContent(long numRecords, List<Map<String, Object>> rows) {}

  private record AddedFile(
      String path,
      long size,
      long modificationTime,
      long numRecords,
      Map<String, String> partitionValues) {}

  private static DeltaLakeSinkDto.Config minimalConfig(Path tableDir) {
    return DeltaLakeSinkDto.Config.builder()
        .tablePath(tableDir.toString())
        .avroSchema(AVRO_SCHEMA)
        .build();
  }

  private static ScalarSinkCommand.SinkResult writeEvents(
      DeltaLakeSinkDto.Config config, JobContext jobContext, List<Map<String, Object>> events)
      throws Exception {
    DeltaLakeSinkCommand command =
        (DeltaLakeSinkCommand)
            new DeltaLakeSinkCommandFactory().createCommand("delta_node", jobContext);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      return command.writeToSink(
          events.stream().map(e -> (RecordFleakData) FleakData.wrap(e)).toList(),
          "test_user",
          command.getExecutionContext());
    } finally {
      command.terminate();
    }
  }

  private static Configuration s3aConfiguration() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.access.key", MINIO_CONTAINER.getUserName());
    hadoopConf.set("fs.s3a.secret.key", MINIO_CONTAINER.getPassword());
    hadoopConf.set("fs.s3a.endpoint", MINIO_CONTAINER.getS3URL());
    hadoopConf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    hadoopConf.set("fs.s3a.path.style.access", "true");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
    // keep this test-side S3A client out of the FileSystem cache so the sink must use its own
    // credentialId-derived configuration
    hadoopConf.set("fs.s3a.impl.disable.cache", "true");
    return hadoopConf;
  }

  private static void createDeltaTable(
      Configuration hadoopConf, String tablePath, List<String> partitionColumns) {
    Engine engine = DefaultEngine.create(hadoopConf);
    TransactionBuilder txnBuilder =
        Table.forPath(engine, tablePath)
            .createTransactionBuilder(engine, "Create test table", Operation.CREATE_TABLE)
            .withSchema(engine, TABLE_SCHEMA);
    if (!partitionColumns.isEmpty()) {
      txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
    }
    txnBuilder.build(engine).commit(engine, emptyCloseableIterable());
  }

  private List<String> s3DeltaLogCommits(String tableKey) {
    return s3Client
        .listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .prefix(tableKey + "/_delta_log/")
                .build())
        .contents()
        .stream()
        .map(S3Object::key)
        .filter(key -> key.endsWith(".json"))
        .map(
            key ->
                s3Client
                    .getObjectAsBytes(
                        GetObjectRequest.builder().bucket(BUCKET_NAME).key(key).build())
                    .asUtf8String())
        .toList();
  }

  private static List<String> localDeltaLogCommits(Path tableDir) throws Exception {
    try (Stream<Path> files = Files.list(tableDir.resolve("_delta_log"))) {
      List<String> commits = new ArrayList<>();
      for (Path commitFile : files.filter(p -> p.toString().endsWith(".json")).toList()) {
        commits.add(Files.readString(commitFile));
      }
      return commits;
    }
  }

  private static TableContent readTableContent(
      Configuration hadoopConf, String tablePath, List<String> deltaLogCommits) throws Exception {
    List<AddedFile> addedFiles = new ArrayList<>();
    for (String commit : deltaLogCommits) {
      for (String line : commit.split("\n")) {
        JsonNode addNode = OBJECT_MAPPER.readTree(line).get("add");
        if (addNode != null) {
          addedFiles.add(
              new AddedFile(
                  addNode.get("path").asText(),
                  addNode.get("size").asLong(),
                  addNode.get("modificationTime").asLong(),
                  OBJECT_MAPPER.readTree(addNode.get("stats").asText()).get("numRecords").asLong(),
                  OBJECT_MAPPER.convertValue(
                      addNode.path("partitionValues"),
                      new TypeReference<Map<String, String>>() {})));
        }
      }
    }

    Engine engine = DefaultEngine.create(hadoopConf);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (AddedFile addedFile : addedFiles) {
      FileStatus fileStatus =
          FileStatus.of(
              tablePath + "/" + addedFile.path(), addedFile.size(), addedFile.modificationTime());
      try (CloseableIterator<ColumnarBatch> batches =
          engine
              .getParquetHandler()
              .readParquetFiles(singletonIterator(fileStatus), TABLE_SCHEMA, Optional.empty())) {
        while (batches.hasNext()) {
          try (CloseableIterator<Row> batchRows = batches.next().getRows()) {
            while (batchRows.hasNext()) {
              Map<String, Object> row = toMap(batchRows.next());
              addedFile.partitionValues().forEach((name, value) -> row.put(name, value));
              rows.add(row);
            }
          }
        }
      }
    }
    rows.sort(Comparator.comparing(row -> (Integer) row.get("id")));
    return new TableContent(addedFiles.stream().mapToLong(AddedFile::numRecords).sum(), rows);
  }

  private static Map<String, Object> toMap(Row row) {
    Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < TABLE_SCHEMA.length(); i++) {
      if (row.isNullAt(i)) {
        continue;
      }
      StructField field = TABLE_SCHEMA.at(i);
      Object value =
          switch (field.getDataType()) {
            case IntegerType ignored -> row.getInt(i);
            case LongType ignored -> row.getLong(i);
            default -> row.getString(i);
          };
      map.put(field.getName(), value);
    }
    return map;
  }

  private static <T> CloseableIterator<T> singletonIterator(T element) {
    return new CloseableIterator<>() {
      private boolean consumed;

      @Override
      public boolean hasNext() {
        return !consumed;
      }

      @Override
      public T next() {
        consumed = true;
        return element;
      }

      @Override
      public void close() {}
    };
  }

  private static <T> CloseableIterable<T> emptyCloseableIterable() {
    return new CloseableIterable<>() {
      @Override
      public CloseableIterator<T> iterator() {
        return new CloseableIterator<>() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public T next() {
            throw new NoSuchElementException();
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public void close() {}
    };
  }
}
