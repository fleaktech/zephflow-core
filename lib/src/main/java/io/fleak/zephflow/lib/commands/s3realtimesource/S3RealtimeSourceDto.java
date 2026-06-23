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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.*;

public interface S3RealtimeSourceDto {

  int DEFAULT_MAX_NUMBER_OF_MESSAGES = 10;
  int MAX_MAX_NUMBER_OF_MESSAGES = 10;
  int DEFAULT_WAIT_TIME_SECONDS = 20;
  int MAX_WAIT_TIME_SECONDS = 20;
  int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;
  long DEFAULT_MAX_OBJECT_SIZE_BYTES = 256L * 1024 * 1024;
  int DEFAULT_MAX_RETRIES = 5;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    /** URL of the SQS queue receiving the S3 event notifications. */
    @NonNull private String queueUrl;

    /** Region of the SQS queue. */
    @NonNull private String regionStr;

    /** Encoding of the referenced S3 objects' content. */
    @NonNull private EncodingType encodingType;

    /**
     * Compression of the referenced S3 objects. When null, gzip is auto-detected from the object's
     * magic bytes (so plain and gzipped objects both work); set {@code GZIP} to force
     * decompression.
     */
    private CompressionType compressionType;

    /** Credential used for both SQS and S3 (falls back to the default AWS credential chain). */
    private String credentialId;

    /**
     * Optional S3 credential override for cross-account buckets; defaults to {@code credentialId}.
     */
    private String s3CredentialId;

    /** Optional S3 region; defaults to {@code regionStr}. */
    private String s3RegionStr;

    /** Optional S3 endpoint override (e.g. MinIO/localstack). */
    private String s3EndpointOverride;

    @Builder.Default private Integer maxNumberOfMessages = DEFAULT_MAX_NUMBER_OF_MESSAGES;
    @Builder.Default private Integer waitTimeSeconds = DEFAULT_WAIT_TIME_SECONDS;
    @Builder.Default private Integer visibilityTimeoutSeconds = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

    /**
     * Objects whose size exceeds this are skipped (and logged) and the notification is
     * acknowledged; the object's content is not downloaded.
     */
    @Builder.Default private Long maxObjectSizeBytes = DEFAULT_MAX_OBJECT_SIZE_BYTES;

    /** In-app retry cap: messages whose ApproximateReceiveCount exceeds this are dead-lettered. */
    @Builder.Default private Integer maxRetries = DEFAULT_MAX_RETRIES;

    /** When true, enrich each emitted record with {@code __s3_bucket} / {@code __s3_key}. */
    @Builder.Default private boolean addS3Metadata = false;
  }
}
