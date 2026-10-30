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
package io.fleak.zephflow.lib.commands.stdout;

import static io.fleak.zephflow.lib.commands.sink.SinkEncoding.field;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_STDOUT;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig.Mode;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Set;

/** Created by bolei on 12/20/24 */
public class StdOutSinkConfigValidator implements ConfigValidator {

  public static final WriterCapability CAPABILITY =
      new WriterCapability(
          COMMAND_NAME_STDOUT,
          Unit.ONE_RECORD,
          Set.of("csv", "json"),
          Set.of(Mode.OBJECT, Mode.ARRAY, Mode.LINES),
          false,
          Set.of());

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    StdOutDto.Config config = (StdOutDto.Config) commandConfig;
    if (config.getEncoding() != null) {
      SinkEncoding.validate(
          config.getEncoding(), CAPABILITY, field("encodingType", config.getEncodingType()));
      return;
    }
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    SerializerFactory.validateEncodingType(config.getEncodingType());
  }
}
