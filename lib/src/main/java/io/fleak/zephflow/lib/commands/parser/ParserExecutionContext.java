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
package io.fleak.zephflow.lib.commands.parser;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import io.fleak.zephflow.lib.parser.CompiledRules;
import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Value;

/** Created by bolei on 3/18/25 */
@EqualsAndHashCode(callSuper = true)
@Value
public class ParserExecutionContext extends DefaultExecutionContext {
  CompiledRules.ParseRule parseRule;

  public ParserExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      CompiledRules.ParseRule parseRule) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.parseRule = parseRule;
  }

  @Override
  public void close() throws IOException {}
}
