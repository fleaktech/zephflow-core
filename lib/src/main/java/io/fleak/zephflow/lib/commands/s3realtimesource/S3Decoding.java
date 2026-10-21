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

import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;

/**
 * The inputs that decide how an S3 object's bytes are decoded: either the legacy {@code
 * encodingType} (with an optional {@code compressionType}) or a {@code decoding} configuration, and
 * whether records get {@code __s3_bucket} / {@code __s3_key}.
 */
public record S3Decoding(
    EncodingType encodingType,
    CompressionType compressionType,
    DecodingConfig decoding,
    boolean addS3Metadata) {

  public static S3Decoding of(S3RealtimeSourceDto.Config config) {
    return new S3Decoding(
        config.getEncodingType(),
        config.getCompressionType(),
        config.getDecoding(),
        config.isAddS3Metadata());
  }

  /** The format's type name, as {@code decoding} spells it ({@code json}, {@code csv}, ...). */
  public String formatName() {
    return sourceDecoding().formatName();
  }

  /**
   * The decoding configuration: {@code decoding} as given, or the legacy settings translated.
   *
   * @throws IllegalArgumentException when not exactly one of {@code encodingType} and {@code
   *     decoding} is set, when {@code compressionType} comes with {@code decoding}, or for {@code
   *     PARQUET}
   */
  public DecodingConfig decodingConfig() {
    return sourceDecoding().decodingConfig();
  }

  private SourceDecoding sourceDecoding() {
    return new SourceDecoding(encodingType, compressionType, decoding);
  }
}
