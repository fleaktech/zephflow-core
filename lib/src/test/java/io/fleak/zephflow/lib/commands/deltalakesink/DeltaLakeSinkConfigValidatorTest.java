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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeSinkDto.Config;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DeltaLakeSinkConfigValidatorTest {

  private final DeltaLakeSinkConfigValidator validator = new DeltaLakeSinkConfigValidator();
  private final JobContext jobContext = Mockito.mock(JobContext.class);

  @Test
  void testValidConfig() {
    Config config = Config.builder().tablePath("/tmp/delta-table").build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testValidS3Path() {
    Config config = Config.builder().tablePath("s3a://bucket/path/to/table").build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testValidHdfsPath() {
    Config config = Config.builder().tablePath("hdfs://namenode:9000/path/to/table").build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testNullTablePath() {
    Config config = Config.builder().tablePath(null).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("tablePath is required"));
  }

  @Test
  void testEmptyTablePath() {
    Config config = Config.builder().tablePath("").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("tablePath is required"));
  }

  @Test
  void testInvalidScheme() {
    Config config = Config.builder().tablePath("ftp://server/path/to/table").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("Unsupported scheme: ftp"));
  }

  @Test
  void testInvalidUriFormat() {
    Config config = Config.builder().tablePath("not a valid uri ://").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("Invalid URI format"));
  }

  @Test
  void testValidBatchSize() {
    Config config = Config.builder().tablePath("/tmp/delta-table").batchSize(2000).build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testInvalidBatchSize() {
    Config config = Config.builder().tablePath("/tmp/delta-table").batchSize(-1).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("batchSize must be positive"));
  }

  @Test
  void testZeroBatchSize() {
    Config config = Config.builder().tablePath("/tmp/delta-table").batchSize(0).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("batchSize must be positive"));
  }

  @Test
  void testExcessiveBatchSize() {
    Config config = Config.builder().tablePath("/tmp/delta-table").batchSize(15000).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("batchSize should not exceed 10,000"));
  }

  @Test
  void testEmptyPartitionColumn() {
    Config config =
        Config.builder()
            .tablePath("/tmp/delta-table")
            .partitionColumns(List.of("valid_column", ""))
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("Partition column names cannot be null or empty"));
  }

  @Test
  void testNullPartitionColumn() {
    Config config =
        Config.builder()
            .tablePath("/tmp/delta-table")
            .partitionColumns(Arrays.asList("valid_column", null))
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("Partition column names cannot be null or empty"));
  }

  @Test
  void testValidPartitionColumns() {
    Config config =
        Config.builder()
            .tablePath("/tmp/delta-table")
            .partitionColumns(List.of("year", "month", "day"))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testEmptyHadoopConfigKey() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("", "value");
    hadoopConfig.put("valid.key", "value");

    Config config =
        Config.builder().tablePath("/tmp/delta-table").hadoopConfiguration(hadoopConfig).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(
        exception.getMessage().contains("Hadoop configuration keys cannot be null or empty"));
  }

  @Test
  void testNullHadoopConfigKey() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put(null, "value");
    hadoopConfig.put("valid.key", "value");

    Config config =
        Config.builder().tablePath("/tmp/delta-table").hadoopConfiguration(hadoopConfig).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(
        exception.getMessage().contains("Hadoop configuration keys cannot be null or empty"));
  }

  @Test
  void testValidHadoopConfiguration() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3a.endpoint", "s3.amazonaws.com");
    hadoopConfig.put("fs.s3a.path.style.access", "true");
    hadoopConfig.put("fs.defaultFS", "s3a://bucket");

    Config config =
        Config.builder()
            .tablePath("s3a://bucket/delta-table")
            .hadoopConfiguration(hadoopConfig)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testS3CredentialsInHadoopConfigurationRejected() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.s3a.access.key", "access-key");
    hadoopConfig.put("fs.s3a.secret.key", "secret-key");
    hadoopConfig.put("fs.s3a.endpoint", "s3.amazonaws.com");

    Config config =
        Config.builder()
            .tablePath("s3a://bucket/delta-table")
            .hadoopConfiguration(hadoopConfig)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "S3 credentials (fs.s3a.access.key, fs.s3a.secret.key) should not be set in hadoopConfiguration"));
    assertTrue(exception.getMessage().contains("Use 'credentialId' field instead"));
  }

  @Test
  void testValidCredentialId() {
    // Mock the job context to have a valid credential
    UsernamePasswordCredential mockCredential =
        new UsernamePasswordCredential("access-key", "secret-key");
    when(jobContext.getOtherProperties()).thenReturn(Map.of("test-credential-id", mockCredential));

    Config config =
        Config.builder()
            .tablePath("s3a://bucket/delta-table")
            .credentialId("test-credential-id")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testInvalidCredentialId() {
    // Mock the job context with no credentials
    when(jobContext.getOtherProperties()).thenReturn(Map.of());

    Config config =
        Config.builder()
            .tablePath("s3a://bucket/delta-table")
            .credentialId("non-existent-credential-id")
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "credentialId 'non-existent-credential-id' was specified but no credential found"));
  }
}
