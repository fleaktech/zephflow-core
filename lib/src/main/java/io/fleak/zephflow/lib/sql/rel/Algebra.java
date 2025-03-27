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

import com.google.common.base.Strings;
import io.fleak.zephflow.lib.sql.ast.QueryAST;
import io.fleak.zephflow.lib.sql.exec.Row;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.*;

/**
 * Our relational algebra is a language of intent of execution. It translates the execution an
 * interpreter would perform into a tree of types. The leaves of the tree are the data sources and
 * each level towards the root is an operation on the data form the leaves. Important; We do not
 * execute the Algebra here but rather build out the execution logic in a procedural way, this
 * allows us to reason about the execution. Put another way, here we are modeling the SQL Execution
 * as a Logical Plan.
 */
@SuppressWarnings("unused")
public class Algebra {

  /**
   * Represents the single instance of a lateral call against a single Row from the relation it's
   * being joined with
   */
  public static Relation lateralRelation(Relation lateralRelation, Row leftRow) {
    return new LateralRowRelation(lateralRelation, leftRow);
  }

  public abstract static class Relation {
    public abstract List<Column> getHeader();
  }

  /**
   * Represents the single instance of joining a Row from the left relation to its lateral join
   * relation. The result should be `len(Relation)` rows with leftRow added. e.g. if `len(Relation)
   * == 3` then the result is: `[leftRow, Relation.row1], [leftRow, Relation.row2], [leftRow,
   * Relation.row3]`
   */
  @Getter
  @AllArgsConstructor
  @ToString
  public static class LateralRowRelation extends Relation {
    private Relation lateralRelation;
    private Object leftRow;

    @Override
    public List<Column> getHeader() {
      return lateralRelation.getHeader();
    }
  }

  @EqualsAndHashCode(callSuper = false)
  @Getter
  @Setter
  @ToString
  public static class Distinct extends NamedRelation {
    private NamedRelation relation;

    public Distinct(NamedRelation relation) {
      super(relation.getName());
      this.relation = relation;
    }

    @Override
    public List<Column> getHeader() {
      return relation.getHeader();
    }
  }

  @EqualsAndHashCode(callSuper = false)
  @Getter
  @Setter
  @ToString
  public static class DistinctOn extends NamedRelation {
    private NamedRelation relation;
    private List<QueryAST.Datum> args;

    public DistinctOn(NamedRelation relation, List<QueryAST.Datum> args) {
      super(relation.getName());
      this.relation = relation;
      this.args = args;
    }

    @Override
    public List<Column> getHeader() {
      return relation.getHeader();
    }
  }

  @AllArgsConstructor
  @EqualsAndHashCode(callSuper = false)
  @Getter
  @ToString
  public abstract static class NamedRelation extends Relation {
    private String name;
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class EmptyRelation extends NamedRelation {
    public EmptyRelation() {
      super("default");
    }

    @Override
    public List<Column> getHeader() {
      return List.of();
    }
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  public static class TableRelation extends NamedRelation {
    private String alias;

    public TableRelation(String name, String alias) {
      super(name);
      this.alias = alias;
    }

    @Override
    public String toString() {
      return "TableRelation{" + "alias='" + alias + '\'' + ", name='" + getName() + '\'' + '}';
    }

    @Override
    public List<Column> getHeader() {
      return List.of();
    }
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class ViewTableRelation extends NamedRelation {
    private Relation relation;

    public ViewTableRelation(Relation relation, String name) {
      super(name);
      this.relation = relation;
    }

    @Override
    public List<Column> getHeader() {
      return List.of();
    }
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class LateralViewTableRelation extends NamedRelation {
    private Relation relation;

    public LateralViewTableRelation(Relation relation, String name) {
      super(name);
      this.relation = relation;
    }

    @Override
    public List<Column> getHeader() {
      return List.of();
    }
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class Project extends NamedRelation {
    private NamedRelation relation;
    private List<Column> names;

    public Project(NamedRelation relation, List<Column> names) {
      super(relation.getName());
      this.relation = relation;
      this.names = names;
    }

    @Override
    public List<Column> getHeader() {
      return names;
    }
  }

  @Getter
  @Setter
  @ToString
  public static class CrossProduct extends NamedRelation {
    private List<NamedRelation> relations;

    public CrossProduct(List<NamedRelation> relations, String name) {
      super(name);
      this.relations = relations;
    }

    /** For cross products we do not use the name in equals tests */
    @Override
    public boolean equals(Object o) {

      // the cross product of a single item is equal to that of the equivalent TableRelation
      if (o instanceof TableRelation && relations.size() == 1) {
        return o.equals(relations.get(0));
      }

      if (o == null || getClass() != o.getClass()) return false;
      CrossProduct that = (CrossProduct) o;

      return Objects.equals(relations, that.relations);
    }

    /** For cross products we do not use the name in the hash */
    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), relations);
    }

    @Override
    public List<Column> getHeader() {
      var allCols = new ArrayList<Column>();
      for (var rel : relations) {
        allCols.addAll(rel.getHeader());
      }
      return allCols;
    }
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class ThetaJoin extends NamedRelation {
    private NamedRelation left;
    private NamedRelation right;
    private Expr<?> expr;

    public ThetaJoin(String name, NamedRelation left, NamedRelation right, Expr<?> expr) {
      super(name);
      this.left = left;
      this.right = right;
      this.expr = expr;
    }

    @Override
    public List<Column> getHeader() {
      var l = new ArrayList<Column>();
      l.addAll(left.getHeader());
      l.addAll(right.getHeader());
      return l;
    }
  }

  @Getter
  @EqualsAndHashCode(callSuper = false)
  public static class Limit extends NamedRelation {
    private final Relation relation;
    private final int n;

    public Limit(NamedRelation relation, int n) {
      super(relation.getName());
      this.relation = relation;
      this.n = n;
    }

    @Override
    public List<Column> getHeader() {
      return relation.getHeader();
    }
  }

  @Getter
  @Setter
  @ToString
  public static class Restrict<E> extends NamedRelation {
    private NamedRelation relation;
    private Expr<E> predicate;

    public Restrict(NamedRelation relation, Expr<E> predicate) {
      super(relation.getName());
      this.relation = relation;
      this.predicate = predicate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      Restrict<E> restrict = (Restrict<E>) o;
      var v1 = Objects.equals(relation, restrict.relation);
      var v2 = Objects.equals(predicate, restrict.predicate);
      return v1 && v2;
    }

    @Override
    public int hashCode() {
      return Objects.hash(relation, predicate);
    }

    @Override
    public List<Column> getHeader() {
      return relation.getHeader();
    }
  }

  @Getter
  @Setter
  @ToString
  public static class Aggregate<E> extends NamedRelation {

    private List<AggregateFunc> aggregateFuncs;
    private GroupBy<E> relation;
    private int selectPosition;
    private List<Column> header;

    public Aggregate(
        String name, GroupBy<E> relation, List<AggregateFunc> aggregateFuncs, int selectPosition) {
      super(relation.getName());
      this.aggregateFuncs = aggregateFuncs;
      this.relation = relation;
      this.selectPosition = selectPosition;

      var header = new ArrayList<>(relation.columns);
      aggregateFuncs.forEach(
          f -> header.add(Algebras.column(name, f.getFuncCall().getName(), Integer.MAX_VALUE)));
      this.header = header;
    }

    @Override
    public List<Column> getHeader() {
      return header;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      Aggregate<?> aggregate = (Aggregate<?>) o;
      return Objects.equals(aggregateFuncs, aggregate.aggregateFuncs)
          && Objects.equals(relation, aggregate.relation)
          && Objects.equals(header, aggregate.header);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), aggregateFuncs, relation, header);
    }
  }

  /**
   * A group contains a relation from which it reads, columns that are used as the group by column
   * keys and group by expressions. a unique key is the combination
   */
  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class GroupBy<E> extends NamedRelation {
    private NamedRelation relation;
    private List<Column> columns;
    // group by supports expressions
    private List<Expr<E>> expressions;
    // group by expressions will be added to this relation name
    private final String extendRelName;
    private final List<Column> header;

    public GroupBy(
        NamedRelation relation,
        List<Column> columns,
        String extendRelName,
        List<Expr<E>> expressions) {
      super(relation.getName());
      this.relation = relation;
      this.columns = columns;
      this.extendRelName = extendRelName;
      this.expressions = expressions;

      var header = new ArrayList<>(columns);
      expressions.forEach(
          e ->
              header.add(
                  Algebras.column(extendRelName, e.getNameOrAlias(), e.getSelectedPosition())));
      this.header = header;
    }

    @Override
    public List<Column> getHeader() {
      return header;
    }
  }

  @AllArgsConstructor
  @EqualsAndHashCode(callSuper = false)
  @Getter
  @Setter
  @ToString
  @SuppressWarnings("all")
  public abstract static class Column {

    /** Keeps the position in the original select statement */
    private int selectPosition = -1;

    public abstract String getName();

    public abstract String getAliasOrName();

    public abstract String getRelName();

    public abstract boolean isAllColumn();

    public abstract RenameField asRenameField(String alias);

    public abstract Column rename(RenameField renameField);

    public abstract boolean canRename(RenameField renameField);

    public abstract Column withAlias(String alias);
  }

  @Setter
  @Getter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class ASTColumn extends Column {
    QueryAST.Column column;

    public ASTColumn(QueryAST.Column column, int selectPosition) {
      super(selectPosition);
      this.column = column;
    }

    @Override
    public String getName() {
      return column.getName();
    }

    @Override
    public String getAliasOrName() {
      return Strings.isNullOrEmpty(column.getAlias()) ? column.getName() : column.getAlias();
    }

    @Override
    public String getRelName() {
      return column.getRelName();
    }

    @Override
    public boolean isAllColumn() {
      return column instanceof QueryAST.AllColumn;
    }

    public RenameField asRenameField(String alias) {
      return new RenameField(this, alias);
    }

    @Override
    public Column rename(RenameField renameField) {
      return new ASTColumn(column.rename(renameField.getNewName()), getSelectPosition());
    }

    @Override
    public boolean canRename(RenameField renameField) {
      return renameField.getColumn().getRelName().equals(getRelName())
          && renameField.getColumn().getName().equals(column.getName());
    }

    @Override
    public Column withAlias(String alias) {
      return new ASTColumn(column.withAlias(alias), getSelectPosition());
    }
  }

  /** sort( Relation, columns=[a desc, b asc] ) */
  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class Sort extends NamedRelation {
    private NamedRelation relation;
    private List<QueryAST.Datum> expressions;

    public Sort(NamedRelation relation, List<QueryAST.Datum> expressions) {
      super(relation.getName());
      this.relation = relation;
      this.expressions = expressions;
    }

    @Override
    public List<Column> getHeader() {
      return relation.getHeader();
    }

    public List<QueryAST.Datum> getExpressions() {
      return expressions;
    }
  }

  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  @ToString
  public static class Rename extends NamedRelation {
    private NamedRelation relation;
    private List<RenameField> renames;

    public Rename(NamedRelation relation, List<RenameField> renames) {
      super(relation.getName());
      this.relation = relation;
      this.renames = renames;
    }

    @Override
    public List<Column> getHeader() {
      var l = new ArrayList<Column>();
      for (var col : relation.getHeader()) {
        var rename = RenameField.findRenameField(col.getRelName(), col.getName(), renames);
        if (rename == null) l.add(col);
        else l.add(Algebras.column(col.getRelName(), col.getName(), col.getSelectPosition()));
      }
      return l;
    }
  }

  @Getter
  @Setter
  @SuppressWarnings("all")
  public static class Extend<E> extends NamedRelation {
    private Relation relation;
    private Expr<E> expr;
    private String colName;

    /** Keeps the position in the original select statement */
    private int selectPosition = -1;

    public Extend(NamedRelation relation, String colName, Expr<E> expr, int selectPosition) {
      super(relation.getName());
      this.colName = colName;
      this.relation = relation;
      this.expr = expr;
      this.selectPosition = selectPosition;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      var extend = (Extend<E>) o;

      var v2 = Objects.equals(relation, extend.relation);
      var v3 = Objects.equals(expr, extend.expr);
      return selectPosition == extend.selectPosition && v2 && v3;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), relation, expr, selectPosition);
    }

    @Override
    public String toString() {
      return "Extend{"
          + "name='"
          + getName()
          + '\''
          + ", selectPosition="
          + selectPosition
          + ", expr="
          + expr
          + ", relation="
          + relation
          + '}';
    }

    @Override
    public List<Column> getHeader() {
      return relation.getHeader();
    }
  }

  public abstract static class Expr<T> {
    public abstract T getDatum();

    public abstract String getNameOrAlias();

    public abstract int getSelectedPosition();
  }

  /**
   * Represents a group of lateral joins in a single query. It takes a Relation source and then for
   * each row in the Relation we add the data from each Relation.
   */
  @Getter
  public static class LateralJoins extends NamedRelation {

    private final Relation source;
    private final List<Relation> laterals;

    public LateralJoins(String name, Relation source, List<Relation> laterals) {
      super(name);
      this.source = source;
      this.laterals = laterals;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LateralJoins that = (LateralJoins) o;
      return Objects.equals(source, that.source) && Objects.equals(laterals, that.laterals);
    }

    @Override
    public int hashCode() {
      return Objects.hash(source, laterals);
    }

    @Override
    @SuppressWarnings("all")
    public List<Column> getHeader() {
      var l = new ArrayList<Column>();
      l.addAll(source.getHeader());
      laterals.forEach(lateral -> l.addAll(lateral.getHeader()));
      return l;
    }

    @Override
    public String toString() {
      return "LateralJoins{"
          + "source="
          + source
          + ", laterals="
          + laterals
          + ", name='"
          + getName()
          + '\''
          + '}';
    }
  }

  @ToString
  @Getter
  public static class AggregateFunc {

    final QueryAST.FuncCall funcCall;
    private final String extendRelName;

    public AggregateFunc(QueryAST.FuncCall funcCall, String extendRelName) {
      this.funcCall = funcCall;
      this.extendRelName = extendRelName;
    }

    public String getName() {
      return funcCall.getName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AggregateFunc that = (AggregateFunc) o;
      return Objects.equals(funcCall, that.funcCall);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(funcCall);
    }
  }

  @Getter
  public static class SetReturningFunc extends NamedRelation {

    final QueryAST.LateralFuncFrom funcFrom;

    public SetReturningFunc(String name, QueryAST.LateralFuncFrom funcFrom) {
      super(name);
      this.funcFrom = funcFrom;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      SetReturningFunc that = (SetReturningFunc) o;
      return Objects.equals(funcFrom, that.funcFrom);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), funcFrom);
    }

    @Override
    public String toString() {
      return "SetReturningFunc{" + "funcFrom=" + funcFrom + ", name='" + getName() + '\'' + '}';
    }

    @Override
    public List<Column> getHeader() {
      return funcFrom.getSchema().stream().map(Algebras::column).collect(Collectors.toList());
    }
  }

  @Getter
  @Setter
  @ToString
  public static class ASTExpr extends Expr<QueryAST.Expr> {
    private QueryAST.Expr expr;

    public ASTExpr(QueryAST.Expr expr) {
      this.expr = expr;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      ASTExpr astExpr = (ASTExpr) o;
      return Objects.equals(expr, astExpr.expr);
    }

    @Override
    public int hashCode() {
      return Objects.hash(expr);
    }

    @Override
    public QueryAST.Expr getDatum() {
      return expr;
    }

    @Override
    public String getNameOrAlias() {
      return expr.getNameOrAlias();
    }

    @Override
    public int getSelectedPosition() {
      return 0;
    }
  }

  @AllArgsConstructor
  @Getter
  @Setter
  @EqualsAndHashCode
  @ToString
  public static class RenameField {
    private Column column;
    private String newName;

    public static Algebra.RenameField findRenameField(
        String rel, String col, List<Algebra.RenameField> renames) {
      for (var f : renames) {
        if (f.column.getRelName().equals(rel) && col.equals(f.column.getName())) return f;
      }
      return null;
    }
  }
}
