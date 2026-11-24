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
package io.fleak.zephflow.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class JobContextTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void s3DlqConfigWithoutCertificateAuthoritiesKeepsTheWireFormatOfOlderRuntimes()
      throws Exception {
    JobContext.DlqConfig dlqConfig =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("dlq-bucket")
            .batchSize(100)
            .flushIntervalMillis(10_000)
            .build();

    JsonNode expected =
        OBJECT_MAPPER.readTree(
            """
            {"type":"s3","region":"us-east-1","bucket":"dlq-bucket","batchSize":100,\
            "flushIntervalMillis":10000,"accessKeyId":null,"secretAccessKey":null,\
            "s3EndpointOverride":null,"rawDataSampleIntervalMs":60000}""");
    assertEquals(expected, OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(dlqConfig)));
  }

  @Test
  void s3DlqConfigWithCertificateAuthoritiesCarriesThem() throws Exception {
    JobContext.DlqConfig dlqConfig =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("dlq-bucket")
            .batchSize(100)
            .flushIntervalMillis(10_000)
            .caCertificates("-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n")
            .build();

    JsonNode expected =
        OBJECT_MAPPER.readTree(
            """
            {"type":"s3","region":"us-east-1","bucket":"dlq-bucket","batchSize":100,\
            "flushIntervalMillis":10000,"accessKeyId":null,"secretAccessKey":null,\
            "s3EndpointOverride":null,\
            "caCertificates":"-----BEGIN CERTIFICATE-----\\nAA==\\n-----END CERTIFICATE-----\\n",\
            "rawDataSampleIntervalMs":60000}""");
    assertEquals(expected, OBJECT_MAPPER.readTree(OBJECT_MAPPER.writeValueAsString(dlqConfig)));
    assertEquals(dlqConfig, OBJECT_MAPPER.treeToValue(expected, JobContext.DlqConfig.class));
  }
}
