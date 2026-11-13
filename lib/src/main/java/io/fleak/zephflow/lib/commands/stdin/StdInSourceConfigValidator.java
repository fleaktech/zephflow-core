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
package io.fleak.zephflow.lib.commands.stdin;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;

/** Created by bolei on 12/20/24 */
public class StdInSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    StdInSourceDto.Config config = (StdInSourceDto.Config) commandConfig;
    DecodingConfig decoding = StdInSourceCommand.decodingConfig(config);
    require(
        !SourceDecoding.multiline(decoding.format()),
        of("decoding", "format", "framing", "type"),
        "stdin reads one message per line and cannot use text multiline framing.",
        "stdin reads one message per line, so decoding cannot use multiline framing");
    // Compiles the decoding, so a bad one fails validation rather than the first message.
    SourceDecoding.validate(decoding);
  }
}
