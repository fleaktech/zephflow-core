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
package io.fleak.zephflow.lib.commands.fssource;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.at;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class FsSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig config, String nodeId, JobContext jobContext) {
    if (!(config instanceof FsSourceDto.Config fsSourceConfig)) {
      throw new IllegalArgumentException("expected FsSourceDto.Config, got " + config.getClass());
    }
    if (fsSourceConfig.getBackend() == null || fsSourceConfig.getBackend().isBlank()) {
      throw at(
          of("backend"),
          new IllegalArgumentException("backend is required"),
          "backend is required and must not be blank.");
    }
    if (fsSourceConfig.getRoot() == null || fsSourceConfig.getRoot().isBlank()) {
      throw at(
          of("root"),
          new IllegalArgumentException("root is required"),
          "root is required and must not be blank.");
    }
    validateExactObjectKey(fsSourceConfig);
    if (fsSourceConfig.getFileNameRegex() != null && !fsSourceConfig.getFileNameRegex().isBlank()) {
      try {
        Pattern.compile(fsSourceConfig.getFileNameRegex());
      } catch (PatternSyntaxException exception) {
        throw at(
            of("fileNameRegex"),
            new IllegalArgumentException(
                "invalid fileNameRegex: " + exception.getMessage(), exception),
            "Invalid regular expression: "
                + exception.getDescription()
                + " at index "
                + exception.getIndex()
                + ".");
      }
    }
    validatePositiveSize("maxFileBytes", fsSourceConfig.getMaxFileBytes());
    validatePositiveSize("chunkSizeBytes", longValueOf(fsSourceConfig.getChunkSizeBytes()));
    validatePositiveSize("maxFilesPerRun", longValueOf(fsSourceConfig.getMaxFilesPerRun()));
    validatePositiveSize("localSpoolMaxBytes", fsSourceConfig.getLocalSpoolMaxBytes());
    // Compiles the decoding, so a bad one fails validation rather than the first file.
    SourceDecoding.validate(decoding(fsSourceConfig).decodingConfig());
  }

  static SourceDecoding decoding(FsSourceDto.Config config) {
    return new SourceDecoding(config.getDecoding());
  }

  private static Long longValueOf(Integer value) {
    return value == null ? null : value.longValue();
  }

  private static void validatePositiveSize(String fieldName, Long value) {
    if (value != null && value <= 0) {
      throw at(
          of(fieldName),
          new IllegalArgumentException(fieldName + " must be greater than 0, got " + value),
          fieldName + " must be greater than 0 when set.");
    }
  }

  private static void validateExactObjectKey(FsSourceDto.Config config) {
    String exactObjectKey = config.getExactObjectKey();
    if (exactObjectKey == null) {
      return;
    }
    if (exactObjectKey.isBlank()) {
      throw at(
          of("exactObjectKey"),
          new IllegalArgumentException("exactObjectKey must not be blank"),
          "exactObjectKey must not be blank when set.");
    }
    if (!"s3".equals(config.getBackend())) {
      throw at(
          of("exactObjectKey"),
          new IllegalArgumentException("exactObjectKey is supported only for the s3 backend"),
          "exactObjectKey can only be set when backend is s3.");
    }
    if (!config.getRoot().startsWith("s3://")) {
      throw at(
          of("root"),
          new IllegalArgumentException("exactObjectKey requires an s3:// root"),
          "root must start with s3:// when exactObjectKey is set.");
    }
    if (exactObjectKey.startsWith("/") || exactObjectKey.startsWith("s3://")) {
      throw at(
          of("exactObjectKey"),
          new IllegalArgumentException("exactObjectKey must be an S3 object key, not a URI"),
          "exactObjectKey must be an S3 object key and must not start with / or s3://.");
    }

    String rootWithoutScheme = config.getRoot().substring("s3://".length());
    int firstSlash = rootWithoutScheme.indexOf('/');
    String rootPrefix = firstSlash < 0 ? "" : rootWithoutScheme.substring(firstSlash + 1);
    if (!rootPrefix.isEmpty() && !rootPrefix.endsWith("/")) {
      rootPrefix += "/";
    }
    if (!exactObjectKey.startsWith(rootPrefix) || exactObjectKey.equals(rootPrefix)) {
      throw at(
          of("exactObjectKey"),
          new IllegalArgumentException("exactObjectKey must be below the configured S3 root"),
          "exactObjectKey must start with the configured S3 root's key prefix followed by a non-empty object key.");
    }
  }
}
