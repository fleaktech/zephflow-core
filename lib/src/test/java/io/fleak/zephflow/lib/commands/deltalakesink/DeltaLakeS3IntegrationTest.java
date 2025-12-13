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
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.type.TypeReference;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.ScalarSinkCommand.SinkResult;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeSinkDto.Config;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * S3 Integration test for Delta Lake sink that writes to a real S3 bucket.
 *
 * <p>NOTE: This test uses a complex nested Avro schema that matches the test data structure.
 *
 * <p>This test is disabled by default. To enable, remove the @Disabled annotation and set the
 * following environment variables:
 *
 * <ul>
 *   <li>DEST_S3_BUCKET - S3 bucket name (e.g., "my-delta-test-bucket")
 *   <li>AWS_ACCESS_KEY_ID - AWS access key
 *   <li>AWS_SECRET_ACCESS_KEY - AWS secret key
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * export DEST_S3_BUCKET=my-delta-test-bucket
 * export AWS_ACCESS_KEY_ID=AKIA...
 * export AWS_SECRET_ACCESS_KEY=...
 * ./gradlew :lib:test --tests "*DeltaLakeS3IntegrationTest*"
 * </pre>
 */
class DeltaLakeS3IntegrationTest {

  private static final Map<String, Object> COMPLEX_AVRO_SCHEMA =
      Map.of(
          "type", "record",
          "name", "ComplexRecord",
          "fields",
              List.of(
                  Map.of("name", "id", "type", "long"),
                  Map.of(
                      "name",
                      "userProfile",
                      "type",
                      Map.of(
                          "type", "record",
                          "name", "UserProfile",
                          "fields",
                              List.of(
                                  Map.of("name", "firstName", "type", "string"),
                                  Map.of("name", "lastName", "type", "string"),
                                  Map.of("name", "email", "type", "string"),
                                  Map.of("name", "employeeId", "type", "long")))),
                  Map.of("name", "department", "type", "string"),
                  Map.of("name", "skills", "type", Map.of("type", "array", "items", "string")),
                  Map.of(
                      "name",
                      "metadata",
                      "type",
                      Map.of(
                          "type", "record",
                          "name", "Metadata",
                          "fields",
                              List.of(
                                  Map.of("name", "created", "type", "long"),
                                  Map.of("name", "version", "type", "string"),
                                  Map.of("name", "source", "type", "string"),
                                  Map.of("name", "priority", "type", "long")))),
                  Map.of("name", "active", "type", "boolean"),
                  Map.of("name", "salary", "type", "double")));

  @Test
  @Disabled("S3 integration test - enable manually and set AWS credentials in environment")
  void testDeltaLakeSinkEndToEndWithS3() {
    // Get configuration from environment
    String bucket = System.getenv("DEST_S3_BUCKET");

    // Create S3 table path (assuming table doesn't exist to test error handling)
    String tablePath =
        "s3a://"
            + bucket
            + "/delta-lake-integration-test/end-to-end-test-"
            + System.currentTimeMillis();

    System.out.println("Testing Delta Lake sink end-to-end with S3: " + tablePath);

    // Step 1: Create real JobContext with S3 credentials in otherProperties
    // S3 uses UsernamePasswordCredential (AWS Access Key ID + Secret Access Key)
    String credentialId = "s3-test-credentials";
    UsernamePasswordCredential s3Credential =
        new UsernamePasswordCredential(
            System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"));

    // Create a real JobContext with credentials in otherProperties
    Map<String, java.io.Serializable> otherProperties = Map.of(credentialId, s3Credential);
    JobContext realJobContext =
        JobContext.builder()
            .otherProperties(otherProperties)
            .metricTags(
                Map.of(
                    METRIC_TAG_SERVICE, "my_service",
                    METRIC_TAG_ENV, "my_env"))
            .build();

    MetricClientProvider metricProvider = new MetricClientProvider.NoopMetricClientProvider();

    // Step 2: Create the Delta Lake sink command using the factory (like ZephFlow would)
    DeltaLakeSinkCommandFactory factory = new DeltaLakeSinkCommandFactory();
    OperatorCommand sinkCommand = factory.createCommand("s3-test-node", realJobContext);

    // Verify command creation
    assertNotNull(sinkCommand);
    assertInstanceOf(DeltaLakeSinkCommand.class, sinkCommand);
    assertInstanceOf(ScalarSinkCommand.class, sinkCommand);

    // Step 3: Configure the command with S3 credentials via credentialId (production pattern)
    Config config =
        Config.builder()
            .tablePath(tablePath)
            .credentialId(credentialId) // Use credential from JobContext
            .batchSize(50) // Smaller batch for integration test
            .avroSchema(COMPLEX_AVRO_SCHEMA)
            .build();

    sinkCommand.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    // Step 4: Create Delta table on S3 for testing
    createDeltaTableOnS3(tablePath, s3Credential);

    // Step 5: Create test RecordFleakData objects with complex structures
    List<RecordFleakData> testEvents =
        List.of(
            createComplexTestRecord(1, "Alice Johnson", "Engineering"),
            createComplexTestRecord(2, "Bob Smith", "Marketing"),
            createComplexTestRecord(3, "Charlie Brown", "Engineering"));

    // Step 6: Execute end-to-end write operation (full ZephFlow pipeline)
    ScalarSinkCommand deltaLakeSink = (ScalarSinkCommand) sinkCommand;
    deltaLakeSink.initialize(metricProvider);
    var context = deltaLakeSink.getExecutionContext();
    SinkResult result = deltaLakeSink.writeToSink(testEvents, "test-user", context);

    // Step 7: Verify successful end-to-end behavior
    assertNotNull(result);
    assertEquals(3, result.getInputCount(), "Should process all 3 input events");
    assertEquals(
        3, result.getSuccessCount(), "Should successfully write all events to S3 Delta table");
    assertEquals(0, result.errorCount(), "Should have no errors for successful S3 write");

    // Verify no errors occurred
    assertTrue(
        result.getFailureEvents().isEmpty(), "Should have no error outputs for successful write");

    System.out.println("✅ End-to-end Delta Lake sink with S3 works perfectly!");
    System.out.println(
        "   Successfully processed and wrote: " + result.getSuccessCount() + " events to S3");
    System.out.println("   S3 table path: " + tablePath);
  }

  @Test
  @Disabled("S3 credential test - enable manually and set AWS credentials in environment")
  void testS3CredentialHandling() throws Exception {
    // Get configuration from environment
    String bucket = System.getenv("DEST_S3_BUCKET");

    // Create real JobContext with S3 credentials
    String credentialId = "s3-credential-test";
    UsernamePasswordCredential s3Credential =
        new UsernamePasswordCredential(
            System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"));

    // Create a real JobContext with credentials in otherProperties
    Map<String, java.io.Serializable> otherProperties = Map.of(credentialId, s3Credential);
    JobContext realJobContext = JobContext.builder().otherProperties(otherProperties).build();

    // Test that credentials are properly applied from JobContext
    Config config =
        Config.builder()
            .tablePath(
                "s3a://"
                    + bucket
                    + "/delta-lake-integration-test/credential-test-"
                    + System.currentTimeMillis())
            .credentialId(credentialId) // Use credential from JobContext
            .avroSchema(COMPLEX_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer =
        new DeltaLakeWriter(
            config,
            realJobContext,
            null,
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            mock(FleakCounter.class));

    // Test initialization with S3 credentials from JobContext
    assertDoesNotThrow(
        writer::initialize, "Should initialize successfully with S3 credentials from JobContext");

    System.out.println("✅ S3 credential handling via JobContext works correctly");

    writer.close();
  }

  /** Creates a complex RecordFleakData object with nested structures and arrays for testing */
  private RecordFleakData createComplexTestRecord(int id, String name, String department) {
    // Create nested user profile with complex structure
    Map<String, FleakData> profilePayload =
        Map.of(
            "firstName", new StringPrimitiveFleakData(name.split(" ")[0]),
            "lastName", new StringPrimitiveFleakData(name.split(" ")[1]),
            "email",
                new StringPrimitiveFleakData(name.toLowerCase().replace(" ", ".") + "@company.com"),
            "employeeId",
                new NumberPrimitiveFleakData(id, NumberPrimitiveFleakData.NumberType.LONG));
    RecordFleakData userProfile = new RecordFleakData(profilePayload);

    // Create array of skills
    List<FleakData> skillsList =
        List.of(
            new StringPrimitiveFleakData("Java"),
            new StringPrimitiveFleakData("Delta Lake"),
            new StringPrimitiveFleakData("AWS"),
            new StringPrimitiveFleakData("Data Engineering"));
    ArrayFleakData skills = new ArrayFleakData(skillsList);

    // Create nested metadata structure
    Map<String, FleakData> metadataPayload =
        Map.of(
            "created",
                new NumberPrimitiveFleakData(
                    System.currentTimeMillis(), NumberPrimitiveFleakData.NumberType.LONG),
            "version", new StringPrimitiveFleakData("1.0"),
            "source", new StringPrimitiveFleakData("integration-test"),
            "priority",
                new NumberPrimitiveFleakData(id % 3 + 1, NumberPrimitiveFleakData.NumberType.LONG));
    RecordFleakData metadata = new RecordFleakData(metadataPayload);

    // Create the main record with complex nested structures
    Map<String, FleakData> rootPayload =
        Map.of(
            "id", new NumberPrimitiveFleakData(id, NumberPrimitiveFleakData.NumberType.LONG),
            "userProfile", userProfile,
            "department", new StringPrimitiveFleakData(department),
            "skills", skills,
            "metadata", metadata,
            "active", new io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData(true),
            "salary",
                new NumberPrimitiveFleakData(
                    50000.0 + (id * 10000), NumberPrimitiveFleakData.NumberType.DOUBLE));

    return new RecordFleakData(rootPayload);
  }

  /** Creates a Delta table on S3 for testing with schema matching our complex test data */
  private void createDeltaTableOnS3(String tablePath, UsernamePasswordCredential s3Credential) {
    System.out.println("Creating Delta table at: " + tablePath);

    // Configure Hadoop for S3 access
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.access.key", s3Credential.getUsername());
    hadoopConf.set("fs.s3a.secret.key", s3Credential.getPassword());
    hadoopConf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    hadoopConf.set("fs.s3a.path.style.access", "false");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "true");

    Engine engine = DefaultEngine.create(hadoopConf);

    // Define schema for the test table matching our complex test data structure
    StructType userProfileSchema =
        new StructType(
            List.of(
                new StructField("firstName", StringType.STRING, true),
                new StructField("lastName", StringType.STRING, true),
                new StructField("email", StringType.STRING, true),
                new StructField("employeeId", LongType.LONG, true)));

    StructType metadataSchema =
        new StructType(
            List.of(
                new StructField("created", LongType.LONG, true),
                new StructField("version", StringType.STRING, true),
                new StructField("source", StringType.STRING, true),
                new StructField("priority", IntegerType.INTEGER, true)));

    StructType mainSchema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("userProfile", userProfileSchema, true),
                new StructField("department", StringType.STRING, true),
                new StructField(
                    "skills", new io.delta.kernel.types.ArrayType(StringType.STRING, true), true),
                new StructField("metadata", metadataSchema, true),
                new StructField("active", BooleanType.BOOLEAN, true),
                new StructField("salary", DoubleType.DOUBLE, true),
                new StructField("_fleak_timestamp", LongType.LONG, true)));

    // Create transaction builder for table creation
    Table table = Table.forPath(engine, tablePath);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(
            engine, "S3 Integration Test Table Creation", Operation.CREATE_TABLE);

    // Set schema for the new table
    txnBuilder = txnBuilder.withSchema(engine, mainSchema);

    // Build and commit the transaction to create the table
    Transaction txn = txnBuilder.build(engine);

    // Create empty CloseableIterable for table creation (no initial data)
    CloseableIterable<io.delta.kernel.data.Row> emptyActions = new EmptyCloseableIterable<>();

    var commitResult = txn.commit(engine, emptyActions);

    System.out.println("✅ Created Delta table at version " + commitResult.getVersion());
  }

  /** Empty CloseableIterable for table creation */
  private static class EmptyCloseableIterable<T> implements CloseableIterable<T> {
    @Override
    public io.delta.kernel.utils.@NotNull CloseableIterator<T> iterator() {
      return new EmptyCloseableIterator<>();
    }

    @Override
    public void close() {
      /* Nothing to close */
    }
  }

  /** Empty CloseableIterator for table creation */
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
    public void close() {
      /* Nothing to close */
    }
  }
}
