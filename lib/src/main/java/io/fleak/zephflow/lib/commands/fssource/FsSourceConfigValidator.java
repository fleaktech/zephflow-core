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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class FsSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig config, String nodeId, JobContext jobContext) {
    if (!(config instanceof FsSourceDto.Config fsSourceConfig)) {
      throw new IllegalArgumentException("expected FsSourceDto.Config, got " + config.getClass());
    }
    if (fsSourceConfig.getBackend() == null || fsSourceConfig.getBackend().isBlank()) {
      throw new IllegalArgumentException("backend is required");
    }
    if (fsSourceConfig.getRoot() == null || fsSourceConfig.getRoot().isBlank()) {
      throw new IllegalArgumentException("root is required");
    }
    validateExactObjectKey(fsSourceConfig);
    if (fsSourceConfig.getFileNameRegex() != null && !fsSourceConfig.getFileNameRegex().isBlank()) {
      try {
        Pattern.compile(fsSourceConfig.getFileNameRegex());
      } catch (PatternSyntaxException exception) {
        throw new IllegalArgumentException(
            "invalid fileNameRegex: " + exception.getMessage(), exception);
      }
    }
    if (fsSourceConfig.getEncodingType() == null) {
      throw new IllegalArgumentException("encodingType is required");
    }
    DeserializerFactory.validateEncodingType(fsSourceConfig.getEncodingType());
  }

  private static void validateExactObjectKey(FsSourceDto.Config config) {
    String exactObjectKey = config.getExactObjectKey();
    if (exactObjectKey == null) {
      return;
    }
    if (exactObjectKey.isBlank()) {
      throw new IllegalArgumentException("exactObjectKey must not be blank");
    }
    if (!"s3".equals(config.getBackend())) {
      throw new IllegalArgumentException("exactObjectKey is supported only for the s3 backend");
    }
    if (!config.getRoot().startsWith("s3://")) {
      throw new IllegalArgumentException("exactObjectKey requires an s3:// root");
    }
    if (exactObjectKey.startsWith("/") || exactObjectKey.startsWith("s3://")) {
      throw new IllegalArgumentException("exactObjectKey must be an S3 object key, not a URI");
    }

    String rootWithoutScheme = config.getRoot().substring("s3://".length());
    int firstSlash = rootWithoutScheme.indexOf('/');
    String rootPrefix = firstSlash < 0 ? "" : rootWithoutScheme.substring(firstSlash + 1);
    if (!rootPrefix.isEmpty() && !rootPrefix.endsWith("/")) {
      rootPrefix += "/";
    }
    if (!exactObjectKey.startsWith(rootPrefix) || exactObjectKey.equals(rootPrefix)) {
      throw new IllegalArgumentException("exactObjectKey must be below the configured S3 root");
    }
  }
}
