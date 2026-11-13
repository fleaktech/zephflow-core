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

import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
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

  private final boolean boundedDiagnostics;

  public PythonFunctionCollector(Map<ParserRuleContext, CompiledPythonFunction> compiledFunctions) {
    this(compiledFunctions, false);
  }

  public PythonFunctionCollector(
      Map<ParserRuleContext, CompiledPythonFunction> compiledFunctions,
      boolean boundedDiagnostics) {
    this.compiledFunctions = compiledFunctions;
    this.boundedDiagnostics = boundedDiagnostics;
  }

  @Override
  public void enterGenericFunctionCall(EvalExpressionParser.GenericFunctionCallContext ctx) {
    // Check if this is a Python function call
    String functionName = ctx.IDENTIFIER().getText();
    if (!"python".equals(functionName)
        || ctx.arguments() == null
        || ctx.arguments().expression().isEmpty()) return;
    // Extract the Python script from the first argument
    String scriptText = decodeScript(ctx.arguments().expression().getFirst().getText());

    try {
      CompiledPythonFunction compiledFunction = compileAndDiscover(scriptText);
      compiledFunctions.put(ctx, compiledFunction);
      if (boundedDiagnostics) {
        log.debug("Pre-compiled Python function.");
      } else {
        log.debug(
            "Pre-compiled Python function at context: {} with script: {}",
            ctx.getSourceInterval(),
            scriptText.substring(0, Math.min(50, scriptText.length())) + "...");
      }
    } catch (PythonExecutor.CleanupFailure cleanup) {
      throw cleanup;
    } catch (ConfigurationValidationException diagnostic) {
      logCompilationFailure(ctx, scriptText, diagnostic);
      throw new ConfigurationValidationException(
          new IllegalArgumentException(diagnostic.getMessage(), diagnostic.originalFailure()),
          diagnostic.issues());
    } catch (Exception failure) {
      logCompilationFailure(ctx, scriptText, failure);
      // Re-throw as IllegalArgumentException so Sentry client error filter can ignore it
      throw new IllegalArgumentException(failure.getMessage(), failure);
    }
  }

  private static String decodeScript(String scriptText) {
    // Handle QUOTED_IDENTIFIER - remove outer quotes and handle escape sequences
    if ((scriptText.startsWith("'") && scriptText.endsWith("'"))
        || (scriptText.startsWith("\"") && scriptText.endsWith("\""))) {
      String unquoted = scriptText.substring(1, scriptText.length() - 1);
      // Handle escape sequences: \n, \t, etc.
      return unquoted
          .replace("\\n", "\n")
          .replace("\\t", "\t")
          .replace("\\r", "\r")
          .replace("\\\\'", "'")
          .replace("\\\"", "\"");
    }

    return scriptText;
  }

  private CompiledPythonFunction compileAndDiscover(String pythonScript) {
    Context pythonContext = createPythonContext();
    boolean entered = false;
    try {
      pythonContext.enter();
      entered = true;
      // 1. Get bindings BEFORE evaluating the user script
      Value initialBindings = pythonContext.getBindings("python");
      Set<String> initialKeys = new HashSet<>(initialBindings.getMemberKeys());

      evaluateScript(pythonContext, pythonScript);
      CompiledPythonFunction function = discoverFunction(pythonContext, initialKeys);
      entered = false;
      pythonContext.leave();
      return function;

    } catch (RuntimeException | Error primary) {
      if (entered) {
        try {
          pythonContext.leave();
        } catch (RuntimeException | Error leaveFailure) {
          suppressCleanupFailure(primary, leaveFailure);
        }
      }
      try {
        pythonContext.close(true);
      } catch (RuntimeException | Error cleanup) {
        if (boundedDiagnostics) {
          PythonExecutor.CleanupFailure failure = new PythonExecutor.CleanupFailure(primary);
          failure.addSuppressed(cleanup);
          throw failure;
        }
        suppressCleanupFailure(primary, cleanup);
      }
      throw primary;
    }
  }

  private static void suppressCleanupFailure(Throwable primary, Throwable cleanup) {
    Throwable original =
        primary instanceof ConfigurationValidationException diagnostic
            ? diagnostic.originalFailure()
            : primary;
    original.addSuppressed(cleanup);
  }

  private void logCompilationFailure(
      EvalExpressionParser.GenericFunctionCallContext context,
      String scriptText,
      Exception failure) {
    if (boundedDiagnostics) {
      log.error("Failed to pre-compile Python function.");
    } else {
      log.error(
          "Failed to pre-compile Python function at {}: {} Script: {}",
          context.getSourceInterval(),
          failure.getMessage(),
          scriptText.substring(0, Math.min(100, scriptText.length())));
    }
  }

  private static void evaluateScript(Context pythonContext, String pythonScript) {
    Source source =
        Source.newBuilder("python", pythonScript, "compileTimeScript.py").buildLiteral();
    try {
      pythonContext.eval(source);
    } catch (PolyglotException failure) {
      if (!failure.isSyntaxError()) throw failure;
      var location = failure.getSourceLocation();
      throw ConfigurationValidationException.at(
          ConfigurationPath.root(),
          failure,
          "Invalid Python syntax"
              + (location == null
                  ? "."
                  : " at line "
                      + location.getStartLine()
                      + ", column "
                      + location.getStartColumn()
                      + "."));
    }
  }

  private CompiledPythonFunction discoverFunction(Context pythonContext, Set<String> initialKeys) {
    Value bindings = pythonContext.getBindings("python");
    List<Value> definedFunctions = new ArrayList<>();
    List<String> definedFunctionNames = new ArrayList<>();
    for (String key : bindings.getMemberKeys()) {
      if (initialKeys.contains(key) || isSpecialMember(key)) continue;
      try {
        Value member = bindings.getMember(key);
        if (member != null && member.canExecute() && !member.isMetaObject()) {
          definedFunctions.add(member);
          definedFunctionNames.add(key);
        }
      } catch (PolyglotException ignored) {
        // Keep discovery tolerant of inaccessible members, as before.
      }
    }
    if (definedFunctions.size() != 1) {
      throw ConfigurationValidationException.at(
          ConfigurationPath.root(),
          new IllegalArgumentException(
              String.format(
                  "Pre-compilation Error: Script must define exactly one function, but defined %d:"
                      + " [%s]",
                  definedFunctions.size(), String.join(", ", definedFunctionNames))),
          "Python script must define exactly one function; found " + definedFunctions.size() + ".");
    }
    if (!boundedDiagnostics) {
      log.debug("Found function defined by script: {}", definedFunctionNames.getFirst());
    }
    return new CompiledPythonFunction(
        definedFunctionNames.getFirst(), definedFunctions.getFirst(), pythonContext);
  }

  private static boolean isSpecialMember(String key) {
    return key.startsWith("__") && key.endsWith("__");
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
