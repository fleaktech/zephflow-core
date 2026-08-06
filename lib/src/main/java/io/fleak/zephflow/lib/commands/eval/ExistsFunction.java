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

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.ArrayAccessStepNode;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.FieldAccessStepNode;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.PathSelectNode;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.PrimaryWithStepsNode;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.StepNode;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.VariableNode;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.util.List;

/*
existsFunction:
Check whether a path is actually present in the event, as opposed to being absent.

Syntax:
```
exists($.path.to.field)
```

A plain comparison cannot answer this question: a missing key and a key whose value is JSON null
both evaluate to null, so `$.a == null` is true for both populations. `exists` looks at the
container instead of the value, so:

Given the input event:
```
{ "a": 1, "b": null }
```

```
exists($.a)          // true
exists($.b)          // true  - the key is there, its value is null
exists($.c)          // false - the key is not there
exists($.a.b)        // false - $.a is not a record, so the key cannot be there
exists($.list[1])    // true when the array holds at least two elements
```

The argument must be a path expression. Anything else is a configuration error, because the
question is only meaningful about a location in the event.
*/
class ExistsFunction implements FeelFunction {

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("exists", 1, "a path expression");
  }

  @Override
  public boolean isLazyEvaluation() {
    return true;
  }

  @Override
  public FleakData evaluateCompiled(
      EvalContext ctx,
      List<ExpressionNode> args,
      EvalExpressionParser.GenericFunctionCallContext originalCtx,
      List<String> lazyArgTexts) {
    ExpressionNode arg = args.getFirst();

    ExpressionNode base;
    List<StepNode> steps;
    if (arg instanceof PathSelectNode pathSelect) {
      base = null;
      steps = pathSelect.steps();
    } else if (arg instanceof PrimaryWithStepsNode primaryWithSteps) {
      base = primaryWithSteps.primary();
      steps = primaryWithSteps.steps();
    } else if (arg instanceof VariableNode) {
      // the bare root '$' compiles to a variable, as does a lambda element variable
      return FleakData.wrap(arg.evaluate(ctx) != null);
    } else {
      throw new IllegalArgumentException(
          "exists expects a path expression such as $.a.b but found: "
              + (lazyArgTexts.isEmpty()
                  ? arg.getClass().getSimpleName()
                  : lazyArgTexts.getFirst()));
    }

    if (steps.isEmpty()) {
      FleakData root = base == null ? ctx.getRootData() : base.evaluate(ctx);
      return FleakData.wrap(root != null);
    }

    FleakData container = base == null ? ctx.getRootData() : base.evaluate(ctx);
    for (int i = 0; i < steps.size() - 1; i++) {
      if (container == null) {
        return FleakData.wrap(false);
      }
      container = steps.get(i).apply(container, ctx);
    }
    if (container == null) {
      return FleakData.wrap(false);
    }

    return FleakData.wrap(stepIsPresent(container, steps.getLast(), ctx));
  }

  private static boolean stepIsPresent(FleakData container, StepNode step, EvalContext ctx) {
    if (step instanceof FieldAccessStepNode fieldAccess) {
      return container instanceof RecordFleakData record
          && record.getPayload().containsKey(fieldAccess.fieldName());
    }
    if (step instanceof ArrayAccessStepNode arrayAccess) {
      if (!(container instanceof ArrayFleakData array)) {
        return false;
      }
      int index = ctx.evalArgAsInt(arrayAccess.indexNode(), "array index");
      return MiscUtils.validArrayIndex(array.getArrayPayload(), index);
    }
    return step.apply(container, ctx) != null;
  }
}
