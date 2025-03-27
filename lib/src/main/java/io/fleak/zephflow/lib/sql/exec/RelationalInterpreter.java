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
package io.fleak.zephflow.lib.sql.exec;

import com.google.common.base.Strings;
import io.fleak.zephflow.lib.sql.exec.operators.*;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import io.fleak.zephflow.lib.sql.rel.Algebras;
import java.util.*;
import java.util.stream.Collectors;

public class RelationalInterpreter<E> {

  final TypeSystem typeSystem;
  final Interpreter<E> exprInterpreter;

  public RelationalInterpreter(TypeSystem typeSystem, Interpreter<E> exprInterpreter) {
    this.typeSystem = typeSystem;
    this.exprInterpreter = exprInterpreter;
  }

  public Iterator<Row> query(Algebra.Relation relation, Catalog catalog) {
    var it = buildIterable(relation, catalog, null);
    return it.iterator();
  }

  @SuppressWarnings("unchecked")
  public Table<Row> buildIterable(Algebra.Relation relation, Catalog catalog, Lookup outerContext) {
    if (relation instanceof Algebra.Project project) {
      var childSource = buildIterable(project.getRelation(), catalog, outerContext);
      return new ProjectExec(
          typeSystem, childSource, resolveProjectNames(childSource, project.getNames()));
    } else if (relation instanceof Algebra.Rename renameRel) {
      var childSource = buildIterable(renameRel.getRelation(), catalog, outerContext);
      return new RenameExec(typeSystem, childSource, renameRel.getRenames());
    } else if (relation instanceof Algebra.TableRelation tableRelation) {
      var tbl = lookupTable(tableRelation, catalog);
      if (!Strings.isNullOrEmpty(tableRelation.getAlias())) {
        return new AliasedTable(typeSystem, tableRelation.getAlias(), tbl);
      }
      return tbl;
    } else if (relation instanceof Algebra.Extend<?> extend) {
      var childSource = buildIterable(extend.getRelation(), catalog, outerContext);
      return new ExtendedTable<>(
          typeSystem,
          childSource,
          childSource.getHeader(),
          exprInterpreter,
          extend.getName(),
          extend.getColName(),
          extend.getSelectPosition(),
          (Algebra.Expr<E>) extend.getExpr(),
          outerContext);
    } else if (relation instanceof Algebra.CrossProduct cross) {
      var childSources =
          cross.getRelations().stream()
              .map(r -> buildIterable(r, catalog, outerContext))
              .collect(Collectors.toList());
      return new CrossProductTable(childSources);
    } else if (relation instanceof Algebra.Restrict<?> restrict) {
      var childSource = buildIterable(restrict.getRelation(), catalog, outerContext);
      return new RestrictTable<>(
          typeSystem,
          childSource,
          childSource.getHeader(),
          exprInterpreter,
          (Algebra.Expr<E>) restrict.getPredicate(),
          outerContext);
    } else if (relation instanceof Algebra.LateralJoins lj) {
      var childSource = buildIterable(lj.getSource(), catalog, outerContext);
      var lateralHeaders = lj.getLaterals().stream().flatMap(r -> r.getHeader().stream()).toList();
      var header = new ArrayList<Algebra.Column>();
      header.addAll(childSource.getHeader());
      header.addAll(lateralHeaders);

      return new LateralJoinsScan<>(
          this, typeSystem, childSource, header, lj.getLaterals(), catalog);
    } else if (relation instanceof Algebra.LateralRowRelation lrr) {
      // children must be correlated
      var row = (Row) lrr.getLeftRow();
      var lateralRel = lrr.getLateralRelation();

      var lateral = buildIterable(lateralRel, catalog, outerContext);
      if (lateral instanceof CorrelatedIterable<?> cll) {
        return new CorrelatedTableScan(null, List.of(), row, (CorrelatedIterable<Row>) cll);
      } else {
        throw new RuntimeException("lateral joins must be correlated but got " + lateralRel);
      }

    } else if (relation instanceof Algebra.SetReturningFunc srf) {
      // return correlated relation function call
      return new CorrelatedFunctionCall<>(srf, exprInterpreter, typeSystem);
    } else if (relation instanceof Algebra.GroupBy<?> grp) {
      var childSource = buildIterable(grp.getRelation(), catalog, outerContext);

      return new GroupByScan(
          this,
          exprInterpreter,
          typeSystem,
          childSource,
          grp.getHeader(),
          Algebras.column(grp.getExtendRelName(), "1", Integer.MAX_VALUE),
          grp,
          catalog);
    } else if (relation instanceof Algebra.Aggregate<?> agg) {
      var childSource = buildIterable(agg.getRelation(), catalog, outerContext);
      return new AggregateScan(
          exprInterpreter, typeSystem, childSource, agg.getHeader(), agg, catalog);
    } else if (relation instanceof Algebra.Limit limit) {
      var childSource = buildIterable(limit.getRelation(), catalog, outerContext);
      return new Limit(typeSystem, childSource, limit.getN());
    } else if (relation instanceof Algebra.EmptyRelation) {
      return new SingleEmptyRowExec(typeSystem);
    } else if (relation instanceof Algebra.ViewTableRelation v) {
      return buildIterable(v.getRelation(), catalog, outerContext);
    } else if (relation instanceof Algebra.LateralViewTableRelation v) {
      // lateral sub select
      return new CorrelatedSubSelect<>(v, this, typeSystem, catalog);
    } else if (relation instanceof Algebra.Distinct distinct) {
      var childSource = buildIterable(distinct.getRelation(), catalog, outerContext);

      return new DistinctRowsScan(childSource);
    } else if (relation instanceof Algebra.DistinctOn distinctOn) {
      var childSource = buildIterable(distinctOn.getRelation(), catalog, outerContext);
      return new DistinctOnRowsScan(exprInterpreter, typeSystem, childSource, distinctOn.getArgs());
    } else if (relation instanceof Algebra.Sort sort) {
      var childSource = buildIterable(sort.getRelation(), catalog, outerContext);
      return new SortScan(exprInterpreter, typeSystem, childSource, sort.getExpressions());
    }

    throw new RuntimeException(relation.getClass() + " is not supported here");
  }

  private List<Algebra.Column> resolveProjectNames(
      Table<Row> childSource, List<Algebra.Column> projectColumns) {
    // here we check for * columns and replace them with the child source columns.
    // this simplifies the project operator and keeps all columns in the whole chain of selects
    // fully qualified.

    var sourceColumns = childSource.getHeader();
    var columns = new ArrayList<Algebra.Column>(projectColumns.size());

    for (var column : projectColumns) {
      if (column.isAllColumn()) {
        columns.addAll(sourceColumns);
      } else {
        columns.add(column);
      }
    }
    return columns;
  }

  private Table<Row> lookupTable(Algebra.TableRelation relation, Catalog catalog) {
    var table = catalog.resolve(relation.getName());
    if (table == null)
      throw new RuntimeException("table " + relation.getName() + " does not exist");

    // else create a view
    return table;
  }
}
