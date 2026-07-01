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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class S3RealtimeSourceDtoTest {

  @Test
  void testJsonParsingWithDefaults() {
    Map<String, Object> jsonMap =
        Map.of(
            "queueUrl",
            "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            "regionStr",
            "us-east-1",
            "encodingType",
            "JSON_OBJECT_LINE");

    S3RealtimeSourceDto.Config config =
        OBJECT_MAPPER.convertValue(jsonMap, S3RealtimeSourceDto.Config.class);

    S3RealtimeSourceDto.Config expected =
        S3RealtimeSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT_LINE)
            .build();
    assertEquals(expected, config);
  }

  @Test
  void testJsonParsingFullConfig() {
    Map<String, Object> jsonMap =
        Map.ofEntries(
            Map.entry("queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"),
            Map.entry("regionStr", "us-east-1"),
            Map.entry("encodingType", "CSV"),
            Map.entry("compressionType", "GZIP"),
            Map.entry("credentialId", "cred"),
            Map.entry("s3CredentialId", "s3cred"),
            Map.entry("s3RegionStr", "us-west-2"),
            Map.entry("s3EndpointOverride", "http://minio:9000"),
            Map.entry("maxNumberOfMessages", 5),
            Map.entry("waitTimeSeconds", 10),
            Map.entry("visibilityTimeoutSeconds", 60),
            Map.entry("maxObjectSizeBytes", 1024),
            Map.entry("maxRetries", 3),
            Map.entry("addS3Metadata", true));

    S3RealtimeSourceDto.Config config =
        OBJECT_MAPPER.convertValue(jsonMap, S3RealtimeSourceDto.Config.class);

    S3RealtimeSourceDto.Config expected =
        S3RealtimeSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.CSV)
            .compressionType(CompressionType.GZIP)
            .credentialId("cred")
            .s3CredentialId("s3cred")
            .s3RegionStr("us-west-2")
            .s3EndpointOverride("http://minio:9000")
            .maxNumberOfMessages(5)
            .waitTimeSeconds(10)
            .visibilityTimeoutSeconds(60)
            .maxObjectSizeBytes(1024L)
            .maxRetries(3)
            .addS3Metadata(true)
            .build();
    assertEquals(expected, config);
  }
}
