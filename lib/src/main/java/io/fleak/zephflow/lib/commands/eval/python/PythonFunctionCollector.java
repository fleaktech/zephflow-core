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
package io.fleak.zephflow.lib.commands.eval.python;

import static io.fleak.zephflow.lib.utils.MiscUtils.normalizeStrLiteral;

import io.fleak.zephflow.lib.antlr.EvalExpressionBaseListener;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import java.util.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.ParserRuleContext;
import org.graalvm.polyglot.*;

/** Created by bolei on 4/22/25 */
@Slf4j
public class PythonFunctionCollector extends EvalExpressionBaseListener {

  @Getter private final Map<ParserRuleContext, CompiledPythonFunction> compiledFunctions;

  public PythonFunctionCollector(Map<ParserRuleContext, CompiledPythonFunction> compiledFunctions) {
    this.compiledFunctions = compiledFunctions;
  }

  @Override
  public void enterPythonFunction(EvalExpressionParser.PythonFunctionContext ctx) {
    // This method is called whenever the walker enters a pythonFunction node

    String pythonScript = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    if (pythonScript == null || pythonScript.isBlank()) {
      throw new IllegalArgumentException(
          String.format(
              "Skipping empty Python script during pre-compilation at %s",
              ctx.getSourceInterval()));
    }

    CompiledPythonFunction compiledFunc = compileAndDiscover(pythonScript);

    // Use the specific node 'ctx' encountered during *this walk* as the key
    compiledFunctions.put(ctx, compiledFunc);
    log.debug(
        "Pre-compiled Python function '{}' for node at {}",
        compiledFunc.discoveredFunctionName(),
        ctx.getSourceInterval());
  }

  private CompiledPythonFunction compileAndDiscover(String pythonScript) {
    Context pythonContext = createPythonContext();
    pythonContext.enter();
    try {
      // 1. Get bindings BEFORE evaluating the user script
      Value initialBindings = pythonContext.getBindings("python");
      Set<String> initialKeys = new HashSet<>(initialBindings.getMemberKeys());

      // 2. Load and evaluate the user script
      Source source =
          Source.newBuilder("python", pythonScript, "compileTimeScript.py").buildLiteral();
      pythonContext.eval(source);

      // 3. Get bindings AFTER evaluating the user script
      Value finalBindings = pythonContext.getBindings("python");
      Set<String> finalKeys = finalBindings.getMemberKeys();

      // 4. Discover the function(s) DEFINED BY THE SCRIPT
      List<Value> definedFunctions = new ArrayList<>();
      List<String> definedFunctionNames = new ArrayList<>();

      for (String key : finalKeys) {
        // Consider only keys that were ADDED by the script
        if (initialKeys.contains(key)) {
          continue;
        }
        if (key.startsWith("__") && key.endsWith("__")) continue;
        try {
          Value member = finalBindings.getMember(key);
          // Ensure it's an executable function defined by the script
          if (member != null && member.canExecute() && !member.isMetaObject()) {
            definedFunctions.add(member);
            definedFunctionNames.add(key);
          }
        } catch (PolyglotException ignored) {
          // Handle or log if needed
        }
      }

      // 5. Validate discovery results based on DEFINED functions
      if (definedFunctions.size() == 1) {
        System.out.println(
            "Found function defined by script: "
                + definedFunctionNames.getFirst()); // Optional logging
        return new CompiledPythonFunction(
            definedFunctionNames.getFirst(), definedFunctions.getFirst(), pythonContext);
      }

      // Log or handle error: 0 or >1 functions *defined by the script* found
      throw new IllegalArgumentException(
          String.format(
              "Pre-compilation Error: Script must define exactly one function, but defined %d: [%s]",
              definedFunctions.size(), String.join(", ", definedFunctionNames)));

    } finally {
      pythonContext.leave();
    }
  }

  private Context createPythonContext() {
    try {
      log.debug("Attempting to initialize GraalVM Python support...");

      Context pythonContext =
          Context.newBuilder("python")
              .allowAllAccess(false) // Deny all privileges by default
              .allowHostAccess(HostAccess.NONE) // No access to host objects
              .allowHostClassLookup(className -> false)
              .allowNativeAccess(false) // No access to native code (JNI/NFI)
              .allowCreateThread(false) // Cannot create host threads
              .allowCreateProcess(false) // Cannot create host processes (e.g., subprocess)
              .allowEnvironmentAccess(EnvironmentAccess.NONE) // No access to environment variables
              .allowPolyglotAccess(PolyglotAccess.NONE) // No access to other languages
              .build();
      log.info("GraalVM Python context created successfully.");
      return pythonContext;
    } catch (Exception e) {
      throw new IllegalArgumentException("failed to create python context", e);
    }
  }
}
