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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class DlqWriterFactoryTest {

  @Test
  void testCreateDlqWriterWithS3Config() throws IOException {
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("test-bucket")
            .batchSize(10)
            .flushIntervalMillis(1000)
            .accessKeyId("testKey")
            .secretAccessKey("testSecret")
            .s3EndpointOverride("http://localhost:9000")
            .build();
    DlqWriter writer = DlqWriterFactory.createDlqWriter(config, "test-prefix");
    assertInstanceOf(S3DlqWriter.class, writer);
    writer.close();
  }

  @Test
  void testCreateSampleWriterWithS3Config() throws IOException {
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("test-bucket")
            .batchSize(10)
            .flushIntervalMillis(1000)
            .accessKeyId("testKey")
            .secretAccessKey("testSecret")
            .s3EndpointOverride("http://localhost:9000")
            .build();
    DlqWriter writer = DlqWriterFactory.createSampleWriter(config, "test-prefix");
    assertInstanceOf(S3DlqWriter.class, writer);
    writer.close();
  }

  @Test
  void testCreateDlqWriterWithUnsupportedConfigThrows() {
    JobContext.DlqConfig unknownConfig = new JobContext.DlqConfig() {};
    assertThrows(
        UnsupportedOperationException.class,
        () -> DlqWriterFactory.createDlqWriter(unknownConfig, "test-prefix"));
  }

  @Test
  void testCreateSampleWriterWithUnsupportedConfigThrows() {
    JobContext.DlqConfig unknownConfig = new JobContext.DlqConfig() {};
    assertThrows(
        UnsupportedOperationException.class,
        () -> DlqWriterFactory.createSampleWriter(unknownConfig, "test-prefix"));
  }
}
