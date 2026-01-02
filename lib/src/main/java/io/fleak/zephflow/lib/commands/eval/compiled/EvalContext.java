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
package io.fleak.zephflow.lib.commands.eval.compiled;

import static io.fleak.zephflow.lib.utils.MiscUtils.ROOT_OBJECT_VARIABLE_NAME;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/** Runtime context for evaluating compiled expressions. Manages variable scopes. */
public record EvalContext(Deque<Map<String, FleakData>> scopes, boolean lenient) {

  public static EvalContext create(FleakData rootData) {
    return create(rootData, false);
  }

  public static EvalContext create(FleakData rootData, boolean lenient) {
    Deque<Map<String, FleakData>> scopes = new LinkedList<>();
    Map<String, FleakData> rootScope = new HashMap<>();
    rootScope.put(ROOT_OBJECT_VARIABLE_NAME, rootData);
    scopes.push(rootScope);
    return new EvalContext(scopes, lenient);
  }

  public FleakData getVariable(String name) {
    for (Map<String, FleakData> scope : scopes) {
      if (scope.containsKey(name)) {
        return scope.get(name);
      }
    }
    throw new RuntimeException("Variable '" + name + "' is not defined");
  }

  public FleakData getRootData() {
    return getVariable(ROOT_OBJECT_VARIABLE_NAME);
  }

  public void enterScope() {
    scopes.push(new HashMap<>());
  }

  public void exitScope() {
    scopes.pop();
  }

  public void setVariable(String name, FleakData value) {
    if (ROOT_OBJECT_VARIABLE_NAME.equals(name)) {
      throw new RuntimeException("Cannot redefine root variable '$'");
    }
    Map<String, FleakData> currentScope = scopes.peek();
    if (currentScope == null) {
      throw new IllegalStateException("No scope available");
    }
    currentScope.put(name, value);
  }

  public int evalArgAsInt(ExpressionNode argNode, String argName) {
    if (argNode == null) {
      throw new IllegalArgumentException("Argument '" + argName + "' is missing");
    }

    FleakData fleakData = argNode.evaluate(this);
    if (!(fleakData instanceof NumberPrimitiveFleakData)) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' must be a number, but got: %s",
              argName, fleakData != null ? fleakData.getClass().getSimpleName() : "null"));
    }

    double doubleValue = getDoubleValue(argName, fleakData);

    return (int) doubleValue;
  }

  private static double getDoubleValue(String argName, FleakData fleakData) {
    double doubleValue = fleakData.getNumberValue();

    if (doubleValue > Integer.MAX_VALUE || doubleValue < Integer.MIN_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' value '%s' is out of valid integer range [%d, %d]",
              argName, doubleValue, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    if (doubleValue % 1 != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' must be a whole number, but got: %s with a fractional part",
              argName, doubleValue));
    }
    return doubleValue;
  }

  public static int fleakDataToInt(FleakData fleakData, String argName) {
    if (!(fleakData instanceof NumberPrimitiveFleakData)) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' must be a number, but got: %s",
              argName, fleakData != null ? fleakData.getClass().getSimpleName() : "null"));
    }

    double doubleValue = getDoubleValue(argName, fleakData);

    return (int) doubleValue;
  }
}
