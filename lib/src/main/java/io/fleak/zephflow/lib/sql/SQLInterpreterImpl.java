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
package io.fleak.zephflow.lib.sql;

import io.fleak.zephflow.lib.sql.ast.QueryAST;
import io.fleak.zephflow.lib.sql.ast.QueryASTParser;
import io.fleak.zephflow.lib.sql.ast.QueryASTRules;
import io.fleak.zephflow.lib.sql.errors.ErrorBeautifier;
import io.fleak.zephflow.lib.sql.errors.SQLRuntimeError;
import io.fleak.zephflow.lib.sql.errors.SQLSyntaxError;
import io.fleak.zephflow.lib.sql.exec.*;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystems;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import io.fleak.zephflow.lib.sql.rel.QueryTranslator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SQLInterpreterImpl<E> implements SQLInterpreter {

  public static final QueryASTInterpreter DEFAULT_EXPRESSION_INTERPRETER =
      Interpreter.astInterpreter(TypeSystems.sqlTypeSystem());
  public static final SQLInterpreterImpl<?> DEFAULT =
      new SQLInterpreterImpl<>(
          TypeSystems.sqlTypeSystem(),
          new RelationalInterpreter<>(TypeSystems.sqlTypeSystem(), DEFAULT_EXPRESSION_INTERPRETER));

  private final TypeSystem typeSystem;
  private final RelationalInterpreter<E> relInterpreter;

  public SQLInterpreterImpl(TypeSystem typeSystem, RelationalInterpreter<E> relInterpreter) {
    this.typeSystem = typeSystem;
    this.relInterpreter = relInterpreter;
  }

  public Stream<Row> eval(Catalog catalog, SQLInterpreter.CompiledQuery query) {
    try {
      CompiledQueryImpl compiledQuery = (CompiledQueryImpl) query;
      var it = relInterpreter.query(compiledQuery.translation, catalog);
      return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
    } catch (Throwable e) {
      throw new SQLRuntimeError(ErrorBeautifier.beautifyRuntimeErrorMessage(e.getMessage()), e);
    }
  }

  public static SQLInterpreterImpl<QueryAST.Datum> create() {
    var typeSystem = TypeSystems.sqlTypeSystem();
    var astInterpreter = Interpreter.astInterpreter(typeSystem);
    return new SQLInterpreterImpl<>(
        typeSystem, new RelationalInterpreter<>(typeSystem, astInterpreter));
  }

  public SQLInterpreter.CompiledQuery compileQuery(String sql) {
    try {
      return new CompiledQueryImpl(typeSystem, sql);
    } catch (Throwable e) {
      throw new SQLSyntaxError(ErrorBeautifier.beautifySyntaxErrorMessage(sql, e.getMessage()), e);
    }
  }

  @Override
  public TypeSystem getTypeSystem() {
    return typeSystem;
  }

  public static class CompiledQueryImpl implements SQLInterpreter.CompiledQuery {
    QueryAST.Query query;
    Algebra.Relation translation;

    public CompiledQueryImpl(TypeSystem typeSystem, String sql) {
      this.query =
          QueryASTRules.applyTypeRules(
              QueryASTParser.astParser().parseSelectStatement(sql), typeSystem);
      // check that all functions exist in the type system.

      this.translation = QueryTranslator.translate(query);
    }
  }
}
