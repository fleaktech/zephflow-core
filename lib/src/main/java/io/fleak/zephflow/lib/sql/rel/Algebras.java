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

import io.fleak.zephflow.lib.sql.ast.QueryAST;
import java.util.List;

public class Algebras {
  public static Algebra.TableRelation table(String name) {
    return new Algebra.TableRelation(name, null);
  }

  public static Algebra.TableRelation table(String name, String alias) {
    return new Algebra.TableRelation(name, alias);
  }

  public static Algebra.LateralJoins lateralJoins(
      Algebra.NamedRelation relation, List<Algebra.Relation> laterals) {
    return new Algebra.LateralJoins(relation.getName(), relation, laterals);
  }

  public static Algebra.NamedRelation empty() {
    return new Algebra.EmptyRelation();
  }

  public static Algebra.ViewTableRelation view(Algebra.Relation relation, String name) {
    return new Algebra.ViewTableRelation(relation, name);
  }

  public static Algebra.ASTExpr astExpr(QueryAST.Expr expr) {
    return new Algebra.ASTExpr(expr);
  }

  public static Algebra.ASTColumn column(QueryAST.SimpleColumn column, int selectPosition) {
    return new Algebra.ASTColumn(column, selectPosition);
  }

  public static Algebra.ASTColumn column(QueryAST.AllColumn column, int selectPosition) {
    return new Algebra.ASTColumn(column, selectPosition);
  }

  public static Algebra.ASTColumn column(String relation, String name, int selectPosition) {
    return new Algebra.ASTColumn(QueryAST.columnFromString(relation, name), selectPosition);
  }

  public static Algebra.ASTColumn column(
      String relation, String name, int selectPosition, String alias) {
    var c = QueryAST.columnFromString(relation, name);
    c.setAlias(alias);
    return new Algebra.ASTColumn(c, selectPosition);
  }

  public static Algebra.Project project(
      Algebra.NamedRelation relation, List<Algebra.Column> names) {
    return new Algebra.Project(relation, names);
  }

  public static Algebra.CrossProduct crossProduct(
      List<Algebra.NamedRelation> relations, String name) {
    return new Algebra.CrossProduct(relations, name);
  }

  public static <E> Algebra.ThetaJoin join(
      String name, Algebra.NamedRelation left, Algebra.NamedRelation right, Algebra.Expr<E> expr) {
    return new Algebra.ThetaJoin(name, left, right, expr);
  }

  public static <E> Algebra.Restrict<E> restrict(
      Algebra.NamedRelation relation, Algebra.Expr<E> predicate) {
    return new Algebra.Restrict<>(relation, predicate);
  }

  public static Algebra.Limit limit(Algebra.NamedRelation relation, int n) {
    return new Algebra.Limit(relation, n);
  }

  public static <E> Algebra.Aggregate<E> aggregate(
      String relationName,
      Algebra.GroupBy groupBy,
      List<Algebra.AggregateFunc> aggregateFuncs,
      int selectPosition) {
    return new Algebra.Aggregate<>(relationName, groupBy, aggregateFuncs, selectPosition);
  }

  public static <E> Algebra.GroupBy<E> group(
      Algebra.NamedRelation relation,
      List<Algebra.Column> groupColumns,
      String extendRelName,
      List<Algebra.Expr<E>> groupExpression) {
    return new Algebra.GroupBy<>(relation, groupColumns, extendRelName, groupExpression);
  }

  public static Algebra.Column column(QueryAST.Column column) {
    return column(column, -1);
  }

  public static Algebra.Column column(QueryAST.Column column, int position) {
    return new Algebra.ASTColumn(column, position);
  }

  @SuppressWarnings("all")
  public static Algebra.Sort sort(Algebra.NamedRelation relation, List<QueryAST.Datum> columns) {
    return new Algebra.Sort(relation, columns);
  }

  public static Algebra.Rename rename(
      Algebra.NamedRelation relation, List<Algebra.RenameField> renames) {
    return new Algebra.Rename(relation, renames);
  }

  public static <E> Algebra.Extend<E> extend(
      Algebra.NamedRelation relation, String name, Algebra.Expr<E> expr, int selectPosition) {
    return new Algebra.Extend<>(relation, name, expr, selectPosition);
  }

  public static Algebra.Expr<QueryAST.Expr> extendExpr(QueryAST.Expr expr) {
    return new Algebra.ASTExpr(expr);
  }

  public static Algebra.RenameField renameField(Algebra.Column column, String newName) {
    return new Algebra.RenameField(column, newName);
  }

  public static Algebra.Distinct distinct(Algebra.NamedRelation rel) {
    return new Algebra.Distinct(rel);
  }

  public static Algebra.NamedRelation distinctOn(
      Algebra.NamedRelation rel, List<QueryAST.Datum> distinctOn) {
    return new Algebra.DistinctOn(rel, distinctOn);
  }

  public static Algebra.AggregateFunc aggregateFunc(QueryAST.FuncCall fn, String extendRelName) {
    return new Algebra.AggregateFunc(fn, extendRelName);
  }
}
