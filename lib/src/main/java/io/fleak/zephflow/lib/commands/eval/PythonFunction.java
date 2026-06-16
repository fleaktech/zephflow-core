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

import static io.fleak.zephflow.lib.utils.GraalUtils.graalValueToFleakData;

import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.python.CompiledPythonFunction;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.GraalUtils;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.*;

/*
pythonFunction:
Execute a single Python function automatically discovered within the script string.

Syntax:
python("<python_script_with_one_function_def>", arg1, arg2, ...)

- The first argument MUST be a string literal containing the Python script.
  This script MUST define exactly one top-level function usable as the entry point.
- Subsequent arguments (arg1, arg2, ...) are standard FEEL expressions whose
  evaluated values will be passed to the discovered Python function.
- Returns the value returned by the Python function, converted back to FEEL data types.
- Throws an error if zero or more than one function is found in the script.
- Requires GraalVM with Python language support configured.
*/
@Slf4j
public record PythonFunction(PythonExecutor pythonExecutor) implements FeelFunction {

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("python", 1, "script string and optional arguments");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (pythonExecutor == null) {
      throw new IllegalArgumentException(
          "cannot execute python() function. No python executor provided");
    }

    if (evaluatedArgs.isEmpty()) {
      throw new IllegalArgumentException("python function expects at least 1 argument (script)");
    }

    CompiledPythonFunction compiledFunc = pythonExecutor.compiledPythonFunctions().get(originalCtx);

    if (compiledFunc == null) {
      throw new IllegalStateException(
          "No pre-compiled Python function found for context: "
              + originalCtx.getSourceInterval()
              + ". Ensure Python functions are pre-compiled before execution.");
    }

    // Skip first argument (script) - already evaluated to string but not used
    List<FleakData> feelArgs = evaluatedArgs.subList(1, evaluatedArgs.size());

    return executePythonFunction(compiledFunc, feelArgs);
  }

  private FleakData executePythonFunction(
      CompiledPythonFunction compiledFunc, List<FleakData> feelArgs) {
    Value targetFunction = compiledFunc.functionValue();

    try {
      // can't close the context here because it will be reused
      // it's closed in pythonExecutor.close()
      @SuppressWarnings("resource")
      Context context = compiledFunc.pythonContext();
      // Convert FEEL arguments to Python arguments
      Object[] pythonArgs =
          feelArgs.stream()
              .map(fd -> fd != null ? fd.unwrap() : null)
              .map(o -> (Object) GraalUtils.convertToPythonValue(context, o))
              .toArray();
      context.enter();
      try {
        Value pyResult = targetFunction.execute(pythonArgs);
        return graalValueToFleakData(pyResult);
      } finally {
        context.leave();
      }
    } catch (PolyglotException e) {
      throw new IllegalArgumentException(
          "Error during execution of Python function: " + e.getMessage(), e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "An unexpected error occurred executing Python function: " + e.getMessage(), e);
    }
  }
}
