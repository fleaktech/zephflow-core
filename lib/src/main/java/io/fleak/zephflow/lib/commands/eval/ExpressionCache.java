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

import static io.fleak.zephflow.lib.utils.MiscUtils.normalizeStrLiteral;

import io.fleak.zephflow.lib.antlr.EvalExpressionLexer;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import java.util.IdentityHashMap;
import java.util.Map;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Caches static values extracted from the parse tree to avoid repeated expensive operations like
 * getText() and normalizeStrLiteral() during expression evaluation.
 *
 * <p>This cache is built once during expression parsing and reused for every event evaluation.
 */
public class ExpressionCache {

  // Cache for normalized string literals (QUOTED_IDENTIFIER tokens)
  private final Map<TerminalNode, String> normalizedStrings = new IdentityHashMap<>();

  // Cache for operator text in binary expressions (the operator child nodes)
  private final Map<ParseTree, String> operatorText = new IdentityHashMap<>();

  // Cache for field names in field access contexts
  private final Map<EvalExpressionParser.FieldAccessContext, String> fieldNames =
      new IdentityHashMap<>();

  // Cache for dict keys
  private final Map<EvalExpressionParser.DictKeyContext, String> dictKeys = new IdentityHashMap<>();

  private ExpressionCache() {}

  /**
   * Build an ExpressionCache by walking the parse tree once and caching all static values.
   *
   * @param root the root of the parse tree
   * @return a populated ExpressionCache
   */
  public static ExpressionCache build(ParseTree root) {
    ExpressionCache cache = new ExpressionCache();
    cache.populateCache(root);
    return cache;
  }

  private void populateCache(ParseTree tree) {
    if (tree == null) {
      return;
    }

    // Cache terminal nodes (QUOTED_IDENTIFIER)
    if (tree instanceof TerminalNode terminalNode) {
      int tokenType = terminalNode.getSymbol().getType();
      if (tokenType == EvalExpressionLexer.QUOTED_IDENTIFIER) {
        String text = terminalNode.getText();
        normalizedStrings.put(terminalNode, normalizeStrLiteral(text));
      }
      // Cache operator text for common operators
      String text = terminalNode.getText();
      if (isOperator(text)) {
        operatorText.put(tree, text);
      }
    }

    // Cache field access context field names
    if (tree instanceof EvalExpressionParser.FieldAccessContext fieldAccessCtx) {
      cacheFieldAccess(fieldAccessCtx);
    }

    // Cache dict key context
    if (tree instanceof EvalExpressionParser.DictKeyContext dictKeyCtx) {
      dictKeys.put(dictKeyCtx, dictKeyCtx.getText());
    }

    // Recursively process children
    for (int i = 0; i < tree.getChildCount(); i++) {
      populateCache(tree.getChild(i));
    }
  }

  private void cacheFieldAccess(EvalExpressionParser.FieldAccessContext ctx) {
    String fieldName;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      fieldName = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    } else if (ctx.IDENTIFIER() != null) {
      fieldName = ctx.IDENTIFIER().getText();
    } else {
      return;
    }
    fieldNames.put(ctx, fieldName);
  }

  private boolean isOperator(String text) {
    return switch (text) {
      case "+", "-", "*", "/", "%", "==", "!=", "<", ">", "<=", ">=", "&&", "||", "!" -> true;
      default -> false;
    };
  }

  /**
   * Get a cached normalized string for a QUOTED_IDENTIFIER terminal node.
   *
   * @param node the terminal node
   * @return the normalized string, or null if not cached
   */
  public String getNormalizedString(TerminalNode node) {
    return normalizedStrings.get(node);
  }

  /**
   * Get cached operator text for a parse tree node.
   *
   * @param node the parse tree node
   * @return the operator text, or null if not cached (will fall back to getText())
   */
  public String getOperator(ParseTree node) {
    String cached = operatorText.get(node);
    if (cached != null) {
      return cached;
    }
    // Fall back to getText() if not cached (shouldn't happen for operators)
    String text = node.getText();
    operatorText.put(node, text);
    return text;
  }

  /**
   * Get cached field name for a FieldAccessContext.
   *
   * @param ctx the field access context
   * @return the field name
   */
  public String getFieldName(EvalExpressionParser.FieldAccessContext ctx) {
    return fieldNames.get(ctx);
  }

  /**
   * Get cached dict key for a DictKeyContext.
   *
   * @param ctx the dict key context
   * @return the dict key text
   */
  public String getDictKey(EvalExpressionParser.DictKeyContext ctx) {
    return dictKeys.get(ctx);
  }
}
