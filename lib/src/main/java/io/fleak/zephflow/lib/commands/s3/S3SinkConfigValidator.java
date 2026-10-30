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

import static io.fleak.zephflow.lib.aws.AwsUtils.parseRegion;
import static io.fleak.zephflow.lib.commands.sink.SinkEncoding.field;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.HashSet;
import java.util.Set;

/** Created by bolei on 9/3/24 */
public class S3SinkConfigValidator implements ConfigValidator {

  public static final WriterCapability CAPABILITY =
      new WriterCapability(
          COMMAND_NAME_S3_SINK,
          Unit.MANY_RECORDS,
          Set.of("csv", "json", "kv", "parquet"),
          Set.of(JsonFormatConfig.Mode.ARRAY, JsonFormatConfig.Mode.LINES),
          false,
          Set.of(DecompressionCodec.GZIP));

  static final Set<EncodingType> SUPPORTED_ENCODING_TYPES;

  static {
    SUPPORTED_ENCODING_TYPES = new HashSet<>(SerializerFactory.SUPPORTED_ENCODING_TYPES);
    SUPPORTED_ENCODING_TYPES.add(EncodingType.PARQUET);
    SUPPORTED_ENCODING_TYPES.add(EncodingType.KV);
  }

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    S3SinkDto.Config config = (S3SinkDto.Config) commandConfig;
    parseRegion(config.getRegionStr());
    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }

    if (!config.isBatching()) {
      throw new IllegalArgumentException(
          "s3sink no longer supports batching false; it always writes one file per batch");
    }
    if (config.getBatchSize() <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "Batch size must be positive when batching is enabled, but found: %d. "
                  + "If omitted, ensure default is applied correctly.",
              config.getBatchSize()));
    }

    if (config.getEncoding() != null) {
      SinkEncoding.validate(
          config.getEncoding(),
          CAPABILITY,
          field("encodingType", config.getEncodingType()),
          field("compressionType", config.getCompressionType()),
          field("avroSchema", config.getAvroSchema()),
          field("kvFields", config.getKvFields()),
          field("kvPairDelimiter", config.getKvPairDelimiter()),
          field("kvCleanFields", config.getKvCleanFields()));
      return;
    }

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    if (!SUPPORTED_ENCODING_TYPES.contains(encodingType)) {
      throw new IllegalArgumentException(
          "Unsupported encoding type for S3 sink: "
              + encodingType
              + ". Supported types: "
              + SUPPORTED_ENCODING_TYPES);
    }

    config.validateKvOptions();

    if (encodingType == EncodingType.JSON_OBJECT) {
      throw new IllegalArgumentException(
          "JSON_OBJECT encoding does not support batching. Use JSON_OBJECT_LINE or JSON_ARRAY instead.");
    }

    if (encodingType == EncodingType.PARQUET) {
      if (config.getAvroSchema() == null || config.getAvroSchema().isEmpty()) {
        throw new IllegalArgumentException("avroSchema is required for PARQUET encoding type");
      }
      if (config.getCompressionType() != null) {
        throw new IllegalArgumentException(
            "compressionType is not supported for PARQUET encoding, which is compressed internally");
      }
    }
  }
}
