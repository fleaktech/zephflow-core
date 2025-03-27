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
package io.fleak.zephflow.lib.commands.eval;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.DefaultInitializedConfig;
import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Value;

/** Created by bolei on 10/19/24 */
@EqualsAndHashCode(callSuper = true)
@Value
public class EvalInitializedConfig extends DefaultInitializedConfig {

  EvalExpressionParser.LanguageContext languageContext;
  boolean assertion;

  public EvalInitializedConfig(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      EvalExpressionParser.LanguageContext languageContext,
      boolean assertion) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.languageContext = languageContext;
    this.assertion = assertion;
  }

  @Override
  public void close() throws IOException {}
}
