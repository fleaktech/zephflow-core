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
package io.fleak.zephflow.lib.sql.rel;

import static io.fleak.zephflow.lib.sql.rel.Algebra.*;

import com.google.common.base.Strings;
import io.fleak.zephflow.lib.sql.ast.QueryAST;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryTranslator {
  private static final AtomicInteger anonymousRelNameCounter = new AtomicInteger();

  public static Algebra.Relation translate(QueryAST.Query query) {
    // Project[TableRelation[table], names=[table.a, table.b]]

    NamedRelation rel;

    String mainRelName =
        query.getFrom().isEmpty() ? anonymousRelName() : query.getFrom().get(0).getAliasOrName();

    rel = translateQueryFrom(query); // from renames happen here
    rel = translateLateralJoins(query, rel);
    rel = translateQueryWhere(query, rel);
    rel = translateGroupBy(query, rel, mainRelName);
    rel = translateAggregate(query, rel, mainRelName);
    rel = translateQueryHaving(query, rel);

    rel = translateQuerySelect(query, rel); // select renames happen here
    rel = translateLimit(query, rel);
    return rel;
  }

  private static NamedRelation translateLimit(QueryAST.Query query, NamedRelation rel) {
    var limit = query.getLimit();
    if (limit.isEmpty()) return rel;

    return Algebras.limit(rel, limit.get());
  }

  private static NamedRelation translateLateralJoins(QueryAST.Query query, NamedRelation rel) {
    var fns = query.getLateralFroms();
    if (!fns.isEmpty()) {
      var lateralJoins = new ArrayList<Relation>();

      for (var latFrom : fns) {
        if (latFrom instanceof QueryAST.LateralFuncFrom lf) {
          lateralJoins.add(new Algebra.SetReturningFunc(lf.getName(), lf));
        } else if (latFrom instanceof QueryAST.SubSelectFrom s) {
          lateralJoins.add(
              new Algebra.LateralViewTableRelation(translate(s.getQuery()), s.getAliasOrName()));
        } else {
          throw new RuntimeException(
              "from clause " + latFrom.getClass() + " is not supported in lateral joins");
        }
      }
      return Algebras.lateralJoins(rel, lateralJoins);
    }
    return rel;
  }

  private static NamedRelation translateQueryHaving(QueryAST.Query query, NamedRelation rel) {
    var having = query.getHaving();
    if (having == null) return rel;

    return Algebras.restrict(rel, Algebras.astExpr(having));
  }

  @SuppressWarnings("unchecked")
  private static <E> NamedRelation translateGroupBy(
      QueryAST.Query query, NamedRelation rel, String extendRelName) {
    // we do not include the function aggregations here

    if (query.getGroupBy().isEmpty()) return rel;

    List<Column> columns = new ArrayList<>();
    List<Expr<E>> exprs = new ArrayList<>();
    for (var groupCol : query.getGroupBy()) {
      if (groupCol instanceof QueryAST.Expr expr) {
        exprs.add((Expr<E>) Algebras.astExpr(expr));

      } else if (groupCol instanceof QueryAST.SimpleColumn) {
        columns.add(Algebras.column((QueryAST.SimpleColumn) groupCol));
      } else {
        throw new RuntimeException(groupCol + " is not supported in group by");
      }
    }

    return Algebras.group(rel, columns, extendRelName, exprs);
  }

  private static NamedRelation translateAggregate(
      QueryAST.Query query, NamedRelation rel, String extendRelName) {

    if (query.getAggregateFunctions().isEmpty()) return rel;

    if (!(rel instanceof GroupBy<?>))
      throw new RuntimeException(
          "When using aggregate functions please specify the group by clause");

    var aggFuncs =
        query.getAggregateFunctions().stream()
            .map(f -> Algebras.aggregateFunc(f, extendRelName))
            .toList();

    return Algebras.aggregate(extendRelName, (GroupBy<?>) rel, aggFuncs, -1);
  }

  private static NamedRelation translateQueryWhere(QueryAST.Query query, NamedRelation rel) {
    if (query.getWhere() == null) return rel;

    // @TODO when adding support for subselects in where, we need to parse the expression
    // for subselects and replace references to them with appropriate calls

    return Algebras.restrict(rel, Algebras.astExpr(query.getWhere()));
  }

  private static NamedRelation translateQuerySelect(QueryAST.Query query, NamedRelation rel) {
    // Separate project and extends columns
    // Some columns are selects from a relation while others generate new data via expressions.
    // --Note that the expressions can also contain selects but this is handled internally by the
    // Extend operator--
    //
    // So we have a list of columns, and split them into "Project" and a call chain of "Extend"
    // select a, 1+2 as b, 1 as c; becomes Extend[Extend[Project[], Expr(1+2), name=b], Expr(1),
    // name=c]

    var columns = query.getColumns();
    boolean hasDistinct = false;
    if (columns.isEmpty()) {
      return rel;
    }

    // use a generator function to capture the recursive Extend calls for each extend column
    Function<NamedRelation, NamedRelation> extendGenerator = (r) -> r;

    List<Column> projectColumns = new ArrayList<>();
    List<RenameField> columnRenames = new ArrayList<>();

    for (int i = 0; i < columns.size(); i++) {
      var column = columns.get(i);

      if (column.isDistinct()) hasDistinct = true;

      if (column instanceof QueryAST.SimpleColumn astColumn) {
        var relColumn = Algebras.column(astColumn, i);
        projectColumns.add(relColumn);
        if (astColumn.hasAlias()) {
          columnRenames.add(Algebras.renameField(relColumn, astColumn.getAlias()));
        }
      } else if (column instanceof QueryAST.AllColumn) {
        var relColumn = Algebras.column((QueryAST.AllColumn) column, i);
        projectColumns.add(relColumn);
      } else if (column instanceof QueryAST.Expr exprColumn) {

        final var columnIndex = i;
        final var finalExtendGen = extendGenerator;

        projectColumns.add(
            Algebras.column(rel.getName(), exprColumn.getNameOrAlias(), columnIndex));

        extendGenerator =
            (r) ->
                Algebras.extend(
                    finalExtendGen.apply(r), // the root relation before the extends
                    assertStringNotOrEmptyNull(
                        exprColumn.getNameOrAlias()), // the extend column name or a generated name
                    Algebras.extendExpr(exprColumn), // the algebra expression
                    columnIndex);
      } else {
        throw new RuntimeException(column + " type is not supported here");
      }
    }

    if (!query.getOrderBy().isEmpty()) {
      // sort the relation before
      rel = Algebras.sort(rel, query.getOrderBy());
    }

    // generate extend chain call with project rel at its root
    rel = extendGenerator.apply(rel);

    if (!projectColumns.isEmpty()) rel = Algebras.project(rel, projectColumns);

    if (!columnRenames.isEmpty()) {
      rel = Algebras.rename(rel, columnRenames);
    }

    if (hasDistinct) {
      rel = Algebras.distinct(rel);
    }

    if (!query.getDistinctOn().isEmpty()) {
      rel = Algebras.distinctOn(rel, query.getDistinctOn());
    }
    return rel;
  }

  private static String assertStringNotOrEmptyNull(String nameOrAlias) {
    if (Strings.isNullOrEmpty(nameOrAlias)) {
      throw new RuntimeException("select columns and expressions must have a name");
    }
    return nameOrAlias;
  }

  private static NamedRelation translateQueryFrom(QueryAST.Query query) {
    var froms = query.getFrom();
    if (froms.size() == 1) {
      return translateFrom(froms.get(0));
    } else if (froms.size() > 1) {
      var rels = froms.stream().map(QueryTranslator::translateFrom).collect(Collectors.toList());
      return Algebras.crossProduct(rels, anonymousRelName());
    }

    return Algebras.empty();
  }

  private static String anonymousRelName() {
    return "rel_" + anonymousRelNameCounter.incrementAndGet();
  }

  public static Algebra.NamedRelation translateFrom(QueryAST.From from) {
    if (from == null) throw new NullPointerException("from cannot be null here");

    if (from instanceof QueryAST.SimpleFrom) {
      return Algebras.table(from.getName(), from.getAlias());
    }

    if (from instanceof QueryAST.SubSelectFrom) {
      var query = ((QueryAST.SubSelectFrom) from).getQuery();
      return Algebras.view(translate(query), from.getAliasOrName());
    }

    // @TODO add support for joins
    //        if (from instanceof QueryAST.JoinFrom) {
    //        }

    throw new RuntimeException(from + " from type is not recognised");
  }
}
