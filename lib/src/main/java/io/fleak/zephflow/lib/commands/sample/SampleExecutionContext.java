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
package io.fleak.zephflow.lib.commands.sample;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;

public class SampleExecutionContext extends DefaultExecutionContext {

  @Getter private final FleakCounter droppedCounter;

  @Getter(AccessLevel.PACKAGE)
  private final List<RuleState> rules;

  @Getter private final String sampleRateField;
  private final List<SampleCondition> conditions;

  public SampleExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      FleakCounter droppedCounter,
      List<RuleState> rules,
      String sampleRateField,
      List<SampleCondition> conditions) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.droppedCounter = droppedCounter;
    this.rules = rules;
    this.sampleRateField = sampleRateField;
    this.conditions = conditions;
  }

  @Override
  public void close() {
    SampleCondition.closeAll(conditions);
  }

  static final class RuleState {
    final CompiledExpression condition;
    final int sampleRate;
    RecordFleakData candidate;
    int count;

    RuleState(CompiledExpression condition, int sampleRate) {
      this.condition = condition;
      this.sampleRate = sampleRate;
    }

    boolean matches(RecordFleakData event) {
      if (condition == null) {
        return true;
      }
      try {
        return condition.evaluate(event) instanceof BooleanPrimitiveFleakData b && b.isTrueValue();
      } catch (Exception e) {
        return false;
      }
    }
  }
}
