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

import static io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.BINARY_VALUE_EVALUATOR_FUNC_MAP;
import static io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.UNARY_VALUE_EVALUATOR_FUNC_MAP;
import static io.fleak.zephflow.lib.utils.GraalUtils.graalValueToFleakData;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionBaseVisitor;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.python.CompiledPythonFunction;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections4.CollectionUtils;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.opensearch.grok.Grok;

/** Created by bolei on 10/19/24 */
@Slf4j
public class ExpressionValueVisitor extends EvalExpressionBaseVisitor<FleakData> {

  private final LinkedList<Map<String, FleakData>> variableEnvironment;

  private final Map<String, Grok> grokCache = new HashMap<>();

  private final boolean lenient;

  private final PythonExecutor pythonExecutor;

  private ExpressionValueVisitor(
      List<Map<String, FleakData>> variableEnvironment,
      boolean lenient,
      PythonExecutor pythonExecutor) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(variableEnvironment));
    this.variableEnvironment = new LinkedList<>(variableEnvironment);
    this.lenient = lenient;
    this.pythonExecutor = pythonExecutor;
  }

  public static ExpressionValueVisitor createInstance(
      FleakData fleakData, PythonExecutor pythonExecutor) {
    return createInstance(fleakData, false, pythonExecutor);
  }

  public static ExpressionValueVisitor createInstance(
      FleakData fleakData, boolean lenient, PythonExecutor pythonExecutor) {
    List<Map<String, FleakData>> variableEnvironment =
        List.of(Map.of(ROOT_OBJECT_VARIABLE_NAME, fleakData));
    return new ExpressionValueVisitor(variableEnvironment, lenient, pythonExecutor);
  }

  @Override
  public FleakData visit(ParseTree tree) {
    try {
      return super.visit(tree);
    } catch (Exception e) {
      if (lenient) {
        return FleakData.wrap(String.format(">>> Evaluation error: %s", e.getMessage()));
      }
      throw e;
    }
  }

  private static long getDuration(String[] parts) {
    int hours = Integer.parseInt(parts[0].trim());
    int minutes = Integer.parseInt(parts[1].trim());
    int seconds = Integer.parseInt(parts[2].trim());

    if (hours < 0) {
      throw new IllegalArgumentException("Hours value cannot be negative");
    }

    if (minutes < 0 || minutes >= 60) {
      throw new IllegalArgumentException("Minutes value must be between 0 and 59");
    }

    if (seconds < 0 || seconds >= 60) {
      throw new IllegalArgumentException("Seconds value must be between 0 and 59");
    }

    return ((hours * 3600L) + (minutes * 60L) + seconds) * 1000L;
  }

  @Override
  public FleakData visitLanguage(EvalExpressionParser.LanguageContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public FleakData visitExpression(EvalExpressionParser.ExpressionContext ctx) {
    return visit(ctx.conditionalOr());
  }

  @Override
  public FleakData visitConditionalOr(EvalExpressionParser.ConditionalOrContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitConditionalAnd(EvalExpressionParser.ConditionalAndContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitEquality(EvalExpressionParser.EqualityContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitRelational(EvalExpressionParser.RelationalContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitAdditive(EvalExpressionParser.AdditiveContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitMultiplicative(EvalExpressionParser.MultiplicativeContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitUnary(EvalExpressionParser.UnaryContext ctx) {
    Preconditions.checkArgument(ctx.getChildCount() == 1 || ctx.getChildCount() == 2);
    // visit the last child to get the value
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    }
    FleakData val = visit(ctx.getChild(1));
    String op = ctx.getChild(0).getText();
    EvaluatorFuncs.UnaryEvaluatorFunc<FleakData> unaryEvaluator =
        Objects.requireNonNull(UNARY_VALUE_EVALUATOR_FUNC_MAP.get(op));
    return unaryEvaluator.evaluate(val);
  }

  @Override
  public FleakData visitPrimary(EvalExpressionParser.PrimaryContext ctx) {
    // First, evaluate the main part of the primary expression
    FleakData value;

    if (ctx.getChildCount() >= 3
        && "(".equals(ctx.getChild(0).getText())
        && ")".equals(ctx.getChild(2).getText())) {
      value = visit(ctx.getChild(1)); // '(' expression ')'
    } else if (ctx.IDENTIFIER() != null) {
      // First alternative: IDENTIFIER (step)*
      String varName = ctx.IDENTIFIER().getText();
      // Retrieve the variable from the current context/environment
      value = getVariableValue(varName);
    } else if (ctx.pathSelectExpr() != null) {
      // Second alternative: pathSelectExpr
      value = visit(ctx.pathSelectExpr());
    } else if (ctx.function() != null) {
      value = visit(ctx.function());
    } else if (ctx.value() != null) {
      value = visit(ctx.value());
    } else if (ctx.caseExpression() != null) {
      value = visit(ctx.caseExpression());
    } else {
      throw new IllegalStateException("Invalid primary reference");
    }

    // Apply any steps if present
    if (CollectionUtils.isEmpty(ctx.step())) {
      return value;
    }

    for (EvalExpressionParser.StepContext stepCtx : ctx.step()) {
      value = applyStep(value, stepCtx);
      if (value == null) {
        return null;
      }
    }

    return value;
  }

  @Override
  public FleakData visitValue(EvalExpressionParser.ValueContext ctx) {
    TerminalNode tn = ctx.QUOTED_IDENTIFIER();
    if (tn != null) {
      String text = tn.getText();
      return visitStrResult(text);
    }
    tn = ctx.BOOLEAN_LITERAL();
    if (tn != null) {
      return visitBoolResult(tn.getText());
    }

    tn = ctx.NULL_LITERAL();
    if (tn != null) {
      return null;
    }

    String numStr;
    tn = ctx.NUMBER_LITERAL();
    if (tn != null) {
      numStr = tn.getText();
    } else {
      numStr = ctx.INT_LITERAL().getText();
    }
    if (numStr.contains(".")) {
      return visitNumResult(Double.parseDouble(numStr), NumberPrimitiveFleakData.NumberType.DOUBLE);
    }
    return visitNumResult(Long.parseLong(numStr), NumberPrimitiveFleakData.NumberType.LONG);
  }

  @Override
  public FleakData visitFunction(EvalExpressionParser.FunctionContext ctx) {
    return visit(ctx.getChild(0));
  }

  @Override
  public FleakData visitDictFunction(EvalExpressionParser.DictFunctionContext ctx) {
    if (ctx.dictArg() == null) {
      return new RecordFleakData();
    }
    return visit(ctx.dictArg());
  }

  @Override
  public FleakData visitDictArg(EvalExpressionParser.DictArgContext ctx) {
    List<FleakData> args = collectDelimitedTreeElements(ctx).stream().map(this::visit).toList();
    Map<String, FleakData> payload = new HashMap<>();
    args.forEach(a -> payload.putAll(a.getPayload()));
    return new RecordFleakData(payload);
  }

  @Override
  public FleakData visitDictMergeFunction(EvalExpressionParser.DictMergeFunctionContext ctx) {
    return visit(ctx.dictMergeArg());
  }

  @Override
  public FleakData visitDictMergeArg(EvalExpressionParser.DictMergeArgContext ctx) {
    List<FleakData> args = collectDelimitedTreeElements(ctx).stream().map(this::visit).toList();
    Map<String, FleakData> payload = new HashMap<>();
    args.forEach(
        fd -> {
          Preconditions.checkArgument(
              fd instanceof RecordFleakData,
              "dict_merge: every argument must be a record but found: %s",
              toJsonString(fd.unwrap()));
          payload.putAll(fd.getPayload());
        });
    return new RecordFleakData(payload);
  }

  @Override
  public FleakData visitKvPair(EvalExpressionParser.KvPairContext ctx) {
    String key = ctx.dictKey().getText();
    FleakData val = visit(ctx.expression());
    HashMap<String, FleakData> payload = new HashMap<>();
    payload.put(key, val);
    return new RecordFleakData(payload);
  }

  @Override
  public FleakData visitArrForEachFunction(EvalExpressionParser.ArrForEachFunctionContext ctx) {
    return visit(ctx.arrForEachArg());
  }

  @Override
  public FleakData visitArrForEachArg(EvalExpressionParser.ArrForEachArgContext ctx) {
    FleakData arrayData = visit(ctx.expression(0));
    if (!(arrayData instanceof ArrayFleakData) && !(arrayData instanceof RecordFleakData)) {
      throw new RuntimeException(
          "path.to.array should point to an array or an object in arr_foreach: "
              + ctx.expression(0).getText());
    }
    if (arrayData instanceof RecordFleakData) {
      arrayData = new ArrayFleakData(List.of(arrayData));
    }
    String elemVarName = ctx.IDENTIFIER().getText();

    List<FleakData> resultArray = new ArrayList<>();
    for (FleakData elem : arrayData.getArrayPayload()) {
      enterScope();
      try {
        setVariable(elemVarName, elem);
        FleakData resultElem = visit(ctx.expression(1));
        resultArray.add(resultElem);
      } finally {
        exitScope();
      }
    }

    return new ArrayFleakData(resultArray);
  }

  @Override
  public FleakData visitArrFlattenFunction(EvalExpressionParser.ArrFlattenFunctionContext ctx) {
    return visit(ctx.arrFlattenArg());
  }

  @Override
  public FleakData visitArrFlattenArg(EvalExpressionParser.ArrFlattenArgContext ctx) {
    FleakData fleakData = visit(ctx.expression());
    Preconditions.checkArgument(
        fleakData instanceof ArrayFleakData,
        "arr_flatten argument must be an array .but found: %s",
        toJsonString(fleakData.unwrap()));
    List<FleakData> arrPayload =
        fleakData.getArrayPayload().stream()
            .peek(
                l ->
                    Preconditions.checkArgument(
                        l instanceof ArrayFleakData,
                        "arr_flatten encountered non array data: %s",
                        toJsonString(l.unwrap())))
            .flatMap(l -> l.getArrayPayload().stream())
            .toList();
    return new ArrayFleakData(arrPayload);
  }

  @Override
  public FleakData visitArrayFunction(EvalExpressionParser.ArrayFunctionContext ctx) {
    if (ctx.arrayArg() == null) {
      return new ArrayFleakData();
    }
    return visit(ctx.arrayArg());
  }

  @Override
  public FleakData visitArrayArg(EvalExpressionParser.ArrayArgContext ctx) {
    List<FleakData> args = collectDelimitedTreeElements(ctx).stream().map(this::visit).toList();
    return new ArrayFleakData(args);
  }

  @Override
  public FleakData visitStrSplitFunction(EvalExpressionParser.StrSplitFunctionContext ctx) {
    return visit(ctx.strSplitArg());
  }

  @Override
  public FleakData visitStrSplitArg(EvalExpressionParser.StrSplitArgContext ctx) {
    FleakData stringData = visit(ctx.expression(0));
    FleakData delimiterData = visit(ctx.expression(1));

    Preconditions.checkArgument(
        stringData instanceof StringPrimitiveFleakData,
        "str_split: first argument must be a string but found: %s",
        toJsonString(stringData != null ? stringData.unwrap() : null));

    Preconditions.checkArgument(
        delimiterData instanceof StringPrimitiveFleakData,
        "str_split: second argument (delimiter) must be a string but found: %s",
        toJsonString(delimiterData != null ? delimiterData.unwrap() : null));

    String inputString = stringData.getStringValue();
    String delimiter = delimiterData.getStringValue();

    if (inputString == null || inputString.isEmpty()) {
      return new ArrayFleakData(List.of());
    }

    String[] parts = inputString.split(delimiter);
    List<FleakData> resultList =
        Arrays.stream(parts).map(StringPrimitiveFleakData::new).collect(toList());

    return new ArrayFleakData(resultList);
  }

  @Override
  public FleakData visitSubStrFunction(EvalExpressionParser.SubStrFunctionContext ctx) {
    return visit(ctx.subStrArg());
  }

  @Override
  public FleakData visitSubStrArg(EvalExpressionParser.SubStrArgContext ctx) {
    List<EvalExpressionParser.ExpressionContext> expressionContexts = ctx.expression();

    FleakData strFd = visit(expressionContexts.get(0));
    FleakData startFd = visit(expressionContexts.get(1));
    FleakData lengthFd = null;

    if (expressionContexts.size() == 3) {
      lengthFd = visit(expressionContexts.get(2));
    }

    // normalize args
    String str = strFd.getStringValue();
    NumberPrimitiveFleakData.NumberType startNumType = startFd.getNumberType();
    Preconditions.checkArgument(
        startNumType == NumberPrimitiveFleakData.NumberType.INT
            || startNumType == NumberPrimitiveFleakData.NumberType.LONG,
        "start position is not a valid integer number: %s",
        startFd);
    int start = (int) startFd.getNumberValue();
    int length = Integer.MAX_VALUE;
    if (lengthFd != null) {
      NumberPrimitiveFleakData.NumberType lengthNumType = lengthFd.getNumberType();
      Preconditions.checkArgument(
          lengthNumType == NumberPrimitiveFleakData.NumberType.INT
              || lengthNumType == NumberPrimitiveFleakData.NumberType.LONG,
          "start position is not a valid integer number: %s",
          lengthFd);
      length = (int) lengthFd.getNumberValue();
    }
    String subStr = subStr(str, start, length);
    return FleakData.wrap(subStr);
  }

  String subStr(String str, int start, int length) {
    if (str == null) {
      throw new IllegalArgumentException("Input string cannot be null");
    }

    if (length < 0) {
      throw new IllegalArgumentException("Length cannot be negative");
    }

    int strLen = str.length();

    // Handle negative start indices (Python-style: -1 is last character)
    if (start < 0) {
      start = strLen + start;

      // If still negative after adjustment, clamp to 0
      if (start < 0) {
        start = 0;
      }
    }

    // Start beyond string length: return empty string
    if (start >= strLen) {
      return "";
    }

    // Calculate the end position, handling potential overflow
    int endPos;
    if (length == Integer.MAX_VALUE || start > Integer.MAX_VALUE - length) {
      // Would overflow, so just use string length
      endPos = strLen;
    } else {
      endPos = start + length;
      // If end position exceeds string length, truncate to string length
      if (endPos > strLen) {
        endPos = strLen;
      }
    }

    // Extract and return the substring
    return str.substring(start, endPos);
  }

  @Override
  public FleakData visitTsStrToEpochFunction(EvalExpressionParser.TsStrToEpochFunctionContext ctx) {
    return visit(ctx.tsStrToEpochArg());
  }

  @Override
  public FleakData visitTsStrToEpochArg(EvalExpressionParser.TsStrToEpochArgContext ctx) {
    FleakData timestampStrFd = visit(ctx.expression());
    Preconditions.checkArgument(
        timestampStrFd instanceof StringPrimitiveFleakData,
        "ts_str_to_epoch: timestamp field to be parsed is not a string: %s",
        timestampStrFd);
    String tsStr = timestampStrFd.getStringValue();
    String patternStr = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    SimpleDateFormat simpleDateFormat;
    try {
      simpleDateFormat = new SimpleDateFormat(patternStr, Locale.US);
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    } catch (Exception e) {
      throw new RuntimeException(
          "ts_str_to_epoch: failed to process date time pattern: " + patternStr);
    }
    try {
      Date date = simpleDateFormat.parse(tsStr);
      return new NumberPrimitiveFleakData(date.getTime(), NumberPrimitiveFleakData.NumberType.LONG);
    } catch (ParseException e) {
      throw new RuntimeException(
          String.format(
              "ts_str_to_epoch: failed to parse timestamp string %s with pattern %s",
              tsStr, patternStr));
    }
  }

  @Override
  public FleakData visitEpochToTsStrFunction(EvalExpressionParser.EpochToTsStrFunctionContext ctx) {
    return visit(ctx.epochToTsStrArg());
  }

  @Override
  public FleakData visitEpochToTsStrArg(EvalExpressionParser.EpochToTsStrArgContext ctx) {
    FleakData epochFd = visit(ctx.expression());
    Preconditions.checkArgument(
        epochFd instanceof NumberPrimitiveFleakData,
        "epoch_to_ts_str: timestamp field to be parsed is not a string: %s",
        epochFd);
    long epoch = (long) epochFd.getNumberValue();
    String patternStr = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    SimpleDateFormat simpleDateFormat;
    try {
      simpleDateFormat = new SimpleDateFormat(patternStr);
    } catch (Exception e) {
      throw new RuntimeException(
          "epoch_to_ts_str: failed to process date time pattern: " + patternStr);
    }
    String tsStr = simpleDateFormat.format(new Date(epoch));
    return new StringPrimitiveFleakData(tsStr);
  }

  @Override
  public FleakData visitDurationStrToMillsFunction(
      EvalExpressionParser.DurationStrToMillsFunctionContext ctx) {
    FleakData durStrFd = visit(ctx.expression());
    Preconditions.checkArgument(
        durStrFd instanceof StringPrimitiveFleakData,
        "duration_str_to_mills: duration argument is not a string: %s",
        durStrFd);
    String durationStr = durStrFd.getStringValue();
    if (durationStr == null || durationStr.isEmpty()) {
      throw new IllegalArgumentException("Duration string cannot be null or empty");
    }

    String[] parts = durationStr.split(":");
    if (parts.length != 3) {
      throw new IllegalArgumentException(
          String.format("Invalid duration format: %s . Expected format is hh:mm:ss", durationStr));
    }

    try {
      long duration = getDuration(parts);
      return new NumberPrimitiveFleakData(duration, NumberPrimitiveFleakData.NumberType.LONG);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Duration string contains non-numeric characters", e);
    }
  }

  @Override
  public FleakData visitParseIntFunction(EvalExpressionParser.ParseIntFunctionContext ctx) {
    return visit(ctx.parseIntArg());
  }

  @Override
  public FleakData visitParseIntArg(EvalExpressionParser.ParseIntArgContext ctx) {
    FleakData valueFd = visit(ctx.expression());
    Preconditions.checkArgument(
        valueFd instanceof StringPrimitiveFleakData,
        "parse_int: argument to be parsed is not a string: %s",
        valueFd);

    String intStr = valueFd.getStringValue();

    int radix = 10;
    if (ctx.INT_LITERAL() != null) {
      String radixStr = ctx.INT_LITERAL().getText();
      radix = Integer.parseInt(radixStr);
    }

    try {
      long value = Long.parseLong(intStr, radix);
      return new NumberPrimitiveFleakData(value, NumberPrimitiveFleakData.NumberType.LONG);
    } catch (Exception e) {
      throw new RuntimeException(
          "parse_int: failed to parse int string: " + intStr + " with radix: " + radix);
    }
  }

  @Override
  public FleakData visitParseFloatFunction(EvalExpressionParser.ParseFloatFunctionContext ctx) {
    FleakData valueFd = visit(ctx.parseFloatArg().expression());
    Preconditions.checkArgument(
        valueFd instanceof StringPrimitiveFleakData,
        "parse_float: argument to be parsed is not a string: %s",
        valueFd);
    String numberStr = valueFd.getStringValue();
    NumberFormat format = NumberFormat.getInstance(Locale.US);
    try {
      Number number = format.parse(numberStr);
      return new NumberPrimitiveFleakData(
          number.doubleValue(), NumberPrimitiveFleakData.NumberType.DOUBLE);
    } catch (Exception e) {
      throw new RuntimeException("parse_float: failed to parse float string: " + numberStr);
    }
  }

  @Override
  public FleakData visitGrokFunction(EvalExpressionParser.GrokFunctionContext ctx) {
    return visit(ctx.grokArg());
  }

  @Override
  public FleakData visitGrokArg(EvalExpressionParser.GrokArgContext ctx) {
    FleakData targetValue = visit(ctx.expression());
    if (targetValue == null) {
      return new RecordFleakData();
    }
    String targetValueStr = targetValue.getStringValue();

    String grokPattern = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    Grok grok =
        grokCache.computeIfAbsent(
            grokPattern, k -> new Grok(Grok.BUILTIN_PATTERNS, k, System.out::println));
    Map<String, Object> map = grok.captures(targetValueStr);
    return FleakData.wrap(map);
  }

  @Override
  public FleakData visitSizeFunction(EvalExpressionParser.SizeFunctionContext ctx) {
    FleakData arg = visit(ctx.expression());
    if (arg instanceof RecordFleakData) {
      return new NumberPrimitiveFleakData(
          arg.getPayload().size(), NumberPrimitiveFleakData.NumberType.INT);
    }
    if (arg instanceof ArrayFleakData) {
      return new NumberPrimitiveFleakData(
          arg.getArrayPayload().size(), NumberPrimitiveFleakData.NumberType.INT);
    }
    if (arg instanceof StringPrimitiveFleakData) {
      return new NumberPrimitiveFleakData(
          arg.getStringValue().length(), NumberPrimitiveFleakData.NumberType.INT);
    }
    throw new IllegalArgumentException("Unsupported argument: " + arg);
  }

  @Override
  public FleakData visitPythonFunction(EvalExpressionParser.PythonFunctionContext ctx) {
    if (pythonExecutor == null) {
      throw new IllegalArgumentException(
          "cannot execute python() function. No python executor provided");
    }

    long startTime = System.nanoTime();

    // 1. Look up the pre-compiled function using the *current node context* as the key
    CompiledPythonFunction compiledFunc = pythonExecutor.getCompiledPythonFunctions().get(ctx);

    if (compiledFunc == null) {
      // This node was not successfully pre-compiled (e.g., script error, discovery failure)
      // Throw an error, as we cannot proceed without the function Value.
      throw new IllegalStateException(
          "No pre-compiled Python function found for the node at: "
              + ctx.getSourceInterval()
              + ". Check pre-compilation logs.");
    }

    Value targetFunction = compiledFunc.functionValue();
    String targetFunctionName = compiledFunc.discoveredFunctionName(); // For logging
    //noinspection resource
    Context context = compiledFunc.pythonContext();

    // 2. Evaluate FEEL arguments for *this specific invocation*
    List<FleakData> feelArgs =
        ctx.expression().stream()
            .map(this::visit) // Use this visitor instance's visit method
            .toList();

    // 3. Unwrap arguments
    Object[] pythonArgs =
        feelArgs.stream()
            .map(fd -> nullOrCompute(fd, FleakData::unwrap))
            .map(
                o -> {
                  if (o instanceof List<?> list) {
                    Value pythonList =
                        context.eval(
                            "python", "[]"); // Executes Python code "[]" to get a list Value

                    for (Object e : list) {
                      pythonList.invokeMember("append", context.asValue(e));
                    }
                    return pythonList;
                  } else {
                    return o;
                  }
                })
            .toArray();

    try {
      // 4. Execute the pre-compiled function Value
      // Use enter/leave for safety if context might be shared across threads
      context.enter();
      Value pyResult;
      try {
        pyResult = targetFunction.execute(pythonArgs);
      } finally {
        context.leave();
      }

      // 5. Convert result
      FleakData result = graalValueToFleakData(pyResult);

      long endTime = System.nanoTime();
      log.debug(
          "Python function '{}' (pre-compiled) execution time: {} ms",
          targetFunctionName,
          (endTime - startTime) / 1_000_000);
      return result;

    } catch (PolyglotException e) {
      throw new IllegalArgumentException(
          "Error during execution of pre-compiled Python function '"
              + targetFunctionName
              + "': "
              + e.getMessage(),
          e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "An unexpected error occurred executing pre-compiled Python function '"
              + targetFunctionName
              + "': "
              + e.getMessage(),
          e);
    }
  }

  private FleakData getVariableValue(String name) {
    for (Map<String, FleakData> scope : variableEnvironment) {
      if (scope.containsKey(name)) {
        return scope.get(name);
      }
    }
    throw new RuntimeException("Variable '" + name + "' is not defined");
  }

  @Override
  public FleakData visitPathSelectExpr(EvalExpressionParser.PathSelectExprContext ctx) {
    FleakData rooObject = getVariableValue(ROOT_OBJECT_VARIABLE_NAME);

    if (CollectionUtils.isEmpty(ctx.step())) {
      return rooObject;
    }

    FleakData value = rooObject;
    for (EvalExpressionParser.StepContext stepCtx : ctx.step()) {
      value = applyStep(value, stepCtx);
    }
    return value;
  }

  @Override
  public FleakData visitCaseExpression(EvalExpressionParser.CaseExpressionContext ctx) {
    for (var whenClause : ctx.whenClause()) {
      FleakData conditionResult = visit(whenClause.expression(0));
      if (conditionResult != null && conditionResult.isTrueValue()) {
        return visit(whenClause.expression(1));
      }
    }
    return visit(ctx.elseClause().expression());
  }

  @Override
  public FleakData visitStrUpperFunction(EvalExpressionParser.StrUpperFunctionContext ctx) {
    FleakData arg = visit(ctx.expression());
    return FleakData.wrap(arg.getStringValue().toUpperCase());
  }

  @Override
  public FleakData visitStrLowerFunction(EvalExpressionParser.StrLowerFunctionContext ctx) {
    FleakData arg = visit(ctx.expression());
    return FleakData.wrap(arg.getStringValue().toLowerCase());
  }

  @Override
  public FleakData visitToStringFunction(EvalExpressionParser.ToStringFunctionContext ctx) {
    FleakData arg = visit(ctx.expression());
    if (arg == null) {
      return null;
    }
    return FleakData.wrap(Objects.toString(arg.unwrap()));
  }

  @Override
  public FleakData visitStrContainsFunction(EvalExpressionParser.StrContainsFunctionContext ctx) {
    EvalExpressionParser.StrContainsArgContext strContainsArgContext = ctx.strContainsArg();
    FleakData val1 = visit(strContainsArgContext.expression(0));
    FleakData val2 = visit(strContainsArgContext.expression(1));
    boolean contains = val1.getStringValue().contains(val2.getStringValue());
    return FleakData.wrap(contains);
  }

  /**
   * Evaluates a range function call, generating a sequence of numbers.
   *
   * <p>Supported forms:
   *
   * <ul>
   *   <li>{@code range(count)}: Generates numbers from 0 up to (but not including) {@code count}.
   *       This is equivalent to {@code range(0, count, 1)}. If {@code count} is 0 or negative, an
   *       empty sequence is produced. Example: {@code range(5)} produces {@code [0, 1, 2, 3, 4]}.
   *       {@code range(-2)} produces {@code []}.
   *   <li>{@code range(start, end)}: Generates numbers from {@code start} up to (but not including)
   *       {@code end}, incrementing by 1. This is equivalent to {@code range(start, end, 1)}. If
   *       {@code start} is greater than or equal to {@code end}, an empty sequence is produced.
   *       Example: {@code range(2, 5)} produces {@code [2, 3, 4]}. {@code range(5, 2)} produces
   *       {@code []}.
   *   <li>{@code range(start, end, step)}: Generates a sequence of numbers starting from {@code
   *       start}. The generation of numbers continues as long as the current number meets the
   *       condition relative to {@code end}, based on the sign of {@code step}:
   *       <ul>
   *         <li>If {@code step} is positive, numbers are generated as long as they are less than
   *             {@code end}.
   *         <li>If {@code step} is negative, numbers are generated as long as they are greater than
   *             {@code end}.
   *       </ul>
   *       The {@code step} argument cannot be zero. If the conditions for generation are not met
   *       initially (e.g., {@code start >= end} with a positive step, or {@code start <= end} with
   *       a negative step), an empty sequence is produced. Examples:
   *       <ul>
   *         <li>{@code range(0, 10, 2)} produces {@code [0, 2, 4, 6, 8]}
   *         <li>{@code range(10, 0, -2)} produces {@code [10, 8, 6, 4, 2]}
   *         <li>{@code range(5, 0, -1)} produces {@code [5, 4, 3, 2, 1]}
   *         <li>{@code range(0, 5, -1)} produces {@code []} (start is not greater than end for
   *             negative step)
   *         <li>{@code range(0, -5, -1)} produces {@code [0, -1, -2, -3, -4]}
   *       </ul>
   * </ul>
   *
   * @param ctx The context for the range function call from the ANTLR parser.
   * @return An {@code ArrayFleakData} instance containing the generated sequence of numbers. Each
   *     number in the sequence is a {@code NumberPrimitiveFleakData} of type LONG.
   * @throws IllegalArgumentException if arguments are invalid (e.g., non-numeric types, step is
   *     zero, or incorrect number of arguments).
   */
  @Override
  public FleakData visitRangeFunction(EvalExpressionParser.RangeFunctionContext ctx) {
    EvalExpressionParser.RangeArgsContext rangeArgsCtx = ctx.rangeArgs();
    List<EvalExpressionParser.ExpressionContext> argExprs = rangeArgsCtx.expression();

    int start = 0;
    int end;
    int step = 1;

    if (argExprs.isEmpty()) {
      throw new IllegalArgumentException("range() function expects at least one argument.");
    }

    if (argExprs.size() == 1) {
      // range(count) => equivalent to range(0, count, 1)
      end = evalArgAsInt(argExprs.get(0), "count");
      // If count is negative, end will be negative. Start is 0, step is 1.
      // Loop condition `i < end` (e.g. `0 < -5`) will be false, resulting in empty list. Correct.
    } else if (argExprs.size() == 2) {
      // range(start, end) => equivalent to range(start, end, 1)
      start = evalArgAsInt(argExprs.get(0), "start");
      end = evalArgAsInt(argExprs.get(1), "end");
    } else if (argExprs.size() == 3) {
      // range(start, end, step)
      start = evalArgAsInt(argExprs.get(0), "start");
      end = evalArgAsInt(argExprs.get(1), "end");
      step = evalArgAsInt(argExprs.get(2), "step");
    } else {
      throw new IllegalArgumentException(
          String.format(
              "range() function called with %d arguments, expects 1, 2, or 3.", argExprs.size()));
    }

    if (step == 0) {
      throw new IllegalArgumentException("range() step argument cannot be zero.");
    }

    List<FleakData> resultNumbers = new ArrayList<>();
    if (step > 0) {
      for (long i = start; i < end; i += step) {
        resultNumbers.add(new NumberPrimitiveFleakData(i, NumberPrimitiveFleakData.NumberType.INT));
      }
    } else { // step < 0
      for (long i = start; i > end; i += step) {
        resultNumbers.add(new NumberPrimitiveFleakData(i, NumberPrimitiveFleakData.NumberType.INT));
      }
    }

    return new ArrayFleakData(resultNumbers);
  }

  private int evalArgAsInt(EvalExpressionParser.ExpressionContext exprCtx, String argName) {
    if (exprCtx == null) {
      throw new IllegalArgumentException(String.format("Argument '%s' is missing", argName));
    }

    FleakData fleakData = visit(exprCtx);
    if (!(fleakData instanceof NumberPrimitiveFleakData)) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' (%s) must be a number, but got: %s",
              argName,
              exprCtx.getText(),
              fleakData != null ? fleakData.getClass().getSimpleName() : "null"));
    }

    NumberPrimitiveFleakData.NumberType numberType = fleakData.getNumberType();
    double doubleValue = fleakData.getNumberValue();

    if (doubleValue > Integer.MAX_VALUE || doubleValue < Integer.MIN_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' (%s) value '%s' is out of the valid integer range [%d, %d].",
              argName, exprCtx.getText(), doubleValue, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    // Check if the number is an integer (e.g., 3.0 is okay, 3.5 is not)
    if (numberType == NumberPrimitiveFleakData.NumberType.DOUBLE
        || numberType == NumberPrimitiveFleakData.NumberType.FLOAT
        || numberType == NumberPrimitiveFleakData.NumberType.UNKNOWN) {

      if (doubleValue % 1 != 0) {
        throw new IllegalArgumentException(
            String.format(
                "Argument '%s' (%s) must be a whole number, but got: %s with a fractional part.",
                argName, exprCtx.getText(), doubleValue));
      }
    }

    return (int) doubleValue;
  }

  private FleakData visitBinaryNode(ParserRuleContext ctx) {
    FleakData value = visit(ctx.getChild(0));
    if (ctx.getChildCount() == 1) {
      return value;
    }
    for (int i = 1; i < ctx.getChildCount(); i += 2) {
      String op = ctx.getChild(i).getText();
      FleakData right = visit(ctx.getChild(i + 1));
      EvaluatorFuncs.BinaryEvaluatorFunc<FleakData> evaluator =
          Objects.requireNonNull(BINARY_VALUE_EVALUATOR_FUNC_MAP.get(op));
      value = evaluator.evaluate(value, right);
    }
    return value;
  }

  private FleakData visitStrResult(String text) {
    String normalized = normalizeStrLiteral(text);
    return new StringPrimitiveFleakData(normalized);
  }

  private FleakData visitBoolResult(String text) {
    return new BooleanPrimitiveFleakData(Boolean.parseBoolean(text));
  }

  private FleakData visitNumResult(
      double numberVal, NumberPrimitiveFleakData.NumberType numberType) {
    return new NumberPrimitiveFleakData(numberVal, numberType);
  }

  private FleakData applyStep(FleakData value, EvalExpressionParser.StepContext ctx) {
    if (ctx.fieldAccess() != null) {
      return applyFieldAccess(value, ctx.fieldAccess());
    } else if (ctx.arrayAccess() != null) {
      return applyArrayAccess(value, ctx.arrayAccess());
    } else {
      throw new RuntimeException("Invalid step in variable reference. Step: " + ctx.getText());
    }
  }

  private FleakData applyFieldAccess(FleakData value, EvalExpressionParser.FieldAccessContext ctx) {

    if (!(value instanceof RecordFleakData)) {
      return null;
    }

    String fieldName;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      fieldName = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    } else if (ctx.IDENTIFIER() != null) {
      fieldName = ctx.IDENTIFIER().getText();
    } else {
      throw new RuntimeException("Invalid field access: " + ctx.getText());
    }

    // Retrieve the field from the current value
    return value.getPayload().get(fieldName);
  }

  private FleakData applyArrayAccess(FleakData value, EvalExpressionParser.ArrayAccessContext ctx) {
    if (!(value instanceof ArrayFleakData)) {
      return null;
    }

    int index = evalArgAsInt(ctx.expression(), "expression");
    if (!validArrayIndex(value.getArrayPayload(), index)) {
      return null;
    }

    return value.getArrayPayload().get(index);
  }

  private void enterScope() {
    variableEnvironment.push(new HashMap<>());
  }

  private void exitScope() {
    variableEnvironment.pop();
  }

  private void setVariable(String name, FleakData value) {
    if ("$".equals(name)) {
      throw new RuntimeException("Cannot redefine root variable '$'");
    }
    Map<String, FleakData> scope = variableEnvironment.peek();
    Preconditions.checkNotNull(scope);
    scope.put(name, value);
  }
}
