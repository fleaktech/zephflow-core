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
package io.fleak.zephflow.lib.commands.filesource;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** Created by bolei on 3/24/25 */
public class FileSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    FileSourceDto.Config config = (FileSourceDto.Config) commandConfig;
    Preconditions.checkNotNull(config.getFilePath(), "file path is missing");
    Path path = Paths.get(config.getFilePath());
    Preconditions.checkArgument(Files.exists(path), "file doesn't exist: %s", config.getFilePath());
    Preconditions.checkArgument(
        Files.isRegularFile(path), "file is not a regular file: %s", config.getFilePath());

    Preconditions.checkNotNull(config.getEncodingType(), "encoding type is missing");
  }
}
