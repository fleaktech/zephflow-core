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
  public void validateConfig(CommandConfig config, String nodeId, JobContext ctx) {
    if (!(config instanceof FsSourceDto.Config c)) {
      throw new IllegalArgumentException("expected FsSourceDto.Config, got " + config.getClass());
    }
    if (c.getBackend() == null || c.getBackend().isBlank()) {
      throw new IllegalArgumentException("backend is required");
    }
    if (c.getRoot() == null || c.getRoot().isBlank()) {
      throw new IllegalArgumentException("root is required");
    }
    if (c.getFileNameRegex() != null && !c.getFileNameRegex().isBlank()) {
      try {
        Pattern.compile(c.getFileNameRegex());
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("invalid fileNameRegex: " + e.getMessage(), e);
      }
    }
    if (c.getEncodingType() == null) {
      throw new IllegalArgumentException("encodingType is required");
    }
    DeserializerFactory.validateEncodingType(c.getEncodingType());
  }
}
