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
package io.fleak.zephflow.lib.sql.ast;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import lombok.*;

public class QueryAST {
  public static final Null NULL = new Null();

  public static Constant constant(Object obj) {
    return new Constant(obj);
  }

  public static Column allColumn(String rel) {
    return new AllColumn(rel, null);
  }

  public static Column columnFromString(String rel, String column) {
    return new SimpleColumn(rel, column);
  }

  public static Column columnFromString(String rel, String field, String alias) {
    var column = new SimpleColumn(rel, field);
    column.setAlias(alias);
    return column;
  }

  public static FuncCall function(String functionName, List<Datum> args) {
    return new FuncCall(functionName, args);
  }

  public static FuncCall function(
      String functionName, List<Datum> args, String name, String alias) {
    var fn = new FuncCall(functionName, args);
    fn.setName(name);
    fn.setAlias(alias);
    return fn;
  }

  public static SimpleFrom fromClauseIdentifier(String rel) {
    return new SimpleFrom(rel);
  }

  public static SubSelectFrom fromClauseQuery(Query q) {
    return new SubSelectFrom(q);
  }

  /**
   * @param funcCall The function call
   * @param relName The relation name we give to the set returned by the function call
   * @param schema The columns that the function are expected to return
   */
  public static LateralFuncFrom funcFrom(FuncCall funcCall, String relName, List<Column> schema) {
    return new LateralFuncFrom(funcCall, relName, schema);
  }

  public static JoinFrom fromJoin(
      DirectionalJoinOp dirOp, CoreJoinOp coreOp, From column, BooleanExpr joinExpr) {
    return new JoinFrom(dirOp, coreOp, joinExpr, column);
  }

  public static BinaryBooleanExpr binaryBoolExpr(
      Datum left, Datum right, BinaryBooleanOperator op) {
    return new BinaryBooleanExpr(left, right, op);
  }

  public static BinaryArithmeticExpr binaryAritmeticExpr(
      Datum left, Datum right, BinaryArithmeticOperator op) {
    return new BinaryArithmeticExpr(left, right, op);
  }

  public static BinaryArithmeticExpr binaryAritmeticExpr(
      Datum left, Datum right, BinaryArithmeticOperator op, String name, String alias) {
    var v = new BinaryArithmeticExpr(left, right, op);
    v.setName(name);
    v.setAlias(alias);
    return v;
  }

  public static UnaryBooleanExpr unaryBooleanExpr(Datum expr, UnaryBooleanOperator op) {
    return new UnaryBooleanExpr(expr, op);
  }

  public static UnaryBooleanExpr isNotNull(Datum expr) {
    return new UnaryBooleanExpr(expr, UnaryBooleanOperator.IS_NOT_NULL);
  }

  public static UnaryBooleanExpr isNull(Datum expr) {
    return new UnaryBooleanExpr(expr, UnaryBooleanOperator.IS_NULL);
  }

  @Data
  public abstract static class Datum {

    /** When renamed a datum can be given an Alias. */
    String alias;

    /** When distinct is used on the expression. */
    boolean distinct;

    /** Only makes sense in an OrderBy clause */
    boolean asc = true;

    /**
     * Allows easy access to all expressions and child expression of a node. This not a DFS not a
     * BFS walk but a simple visit parent then visit children pattern, and should cover most use
     * cases where we want to visit all nodes of a certain type.
     */
    public abstract void walk(Consumer<Datum> walk);

    public boolean hasAlias() {
      return !Strings.isNullOrEmpty(alias);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class Null extends Datum {

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
    }
  }

  @Data
  public static class Constant extends Expr {
    Object value;

    public Constant(Object value) {
      this.value = value;
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Constant constant)) return false;

      var thatValue = constant.value;
      var value = this.value;
      if (value instanceof Double && thatValue instanceof Number
          || (thatValue instanceof Double && value instanceof Number)) {
        return Math.abs(((Number) value).doubleValue() - ((Number) thatValue).doubleValue())
            < 0.000001d;
      } else if (value instanceof Number && thatValue instanceof Number) {
        return ((Number) value).longValue() == ((Number) thatValue).longValue();
      }
      return Objects.equals(value, constant.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), value);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class Query extends Datum {
    private List<From> from = new ArrayList<>();

    private BooleanExpr where;
    private BooleanExpr having;
    private List<Datum> columns = new ArrayList<>();
    private List<Datum> groupBy = new ArrayList<>();
    private List<Datum> orderBy = new ArrayList<>();
    private List<FuncCall> aggregateFunctions = new ArrayList<>();
    private List<Datum> distinctOn = new ArrayList<>();

    // Function calls here need to branch from the from clauses and their results are joined using a
    // natural join with no common key
    // e.g [a,b] merge [1,2,3,4] => [a, 1] [b, 2], [null, 3], [null, 4]
    private List<From> lateralFroms = new ArrayList<>();

    private String name;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Optional<Integer> limit = Optional.empty();

    public void setDistinctOn(List<Datum> distinctOn) {
      this.distinctOn.addAll(distinctOn);
    }

    @Nonnull
    public List<Datum> getDistinctOn() {
      return distinctOn;
    }

    public void addJoins(List<JoinFrom> joins) {
      from.addAll(joins);
    }

    public void updateLateralFroms(List<From> laterals) {
      this.lateralFroms.addAll(laterals);
    }

    public void updateAggregateFunctions(List<FuncCall> aggregateFunctions) {
      this.aggregateFunctions.addAll(aggregateFunctions);
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);

      from.forEach(f -> f.walk(walk));

      lateralFroms.forEach(f -> f.walk(walk));

      if (where != null) {
        where.walk(walk);
      }

      if (having != null) {
        having.walk(walk);
      }

      groupBy.forEach(f -> f.walk(walk));
      orderBy.forEach(f -> f.walk(walk));
      columns.forEach(f -> f.walk(walk));
      aggregateFunctions.forEach(f -> f.walk(walk));
      distinctOn.forEach(f -> f.walk(walk));
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public abstract static class From extends Datum {
    private String name;

    @SuppressWarnings("unused")
    public From() {}

    public From(String name) {
      this.name = name;
    }

    /** If alias is defined return alias otherwise return name */
    public String getAliasOrName() {
      return Strings.isNullOrEmpty(alias) ? getName() : getAlias();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class SimpleFrom extends From {
    public SimpleFrom(String name) {
      super(name);
    }

    @Override
    public String toString() {
      return "SimpleFrom{" + "name='" + super.name + '\'' + ", alias='" + super.alias + '\'' + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class SubSelectFrom extends From {
    private Query query;

    public SubSelectFrom(Query query) {
      super(query.getName());
      this.query = query;
    }

    @Override
    public String toString() {
      return "SubSelectFrom{"
          + "query="
          + query
          + ", name='"
          + super.name
          + '\''
          + ", alias='"
          + super.alias
          + '\''
          + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      query.walk(walk);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class JoinFrom extends From {
    private DirectionalJoinOp directionalJoinOp;
    private CoreJoinOp coreJoinOpp;

    private Expr joinExpr;

    /**
     * Can be either a SimpleFrom as in join table on ... or a select join as in join (select * from
     * table) t on
     *
     * <p>In the first case we have a SimpleFrom and in the last a SubSelectFrom
     */
    private From from;

    public JoinFrom(
        DirectionalJoinOp directionalJoinOp, CoreJoinOp coreJoinOpp, Expr joinExpr, From from) {
      super(from.name);
      super.setAlias(from.getAlias());
      this.directionalJoinOp = directionalJoinOp;
      this.coreJoinOpp = coreJoinOpp;
      this.joinExpr = joinExpr;
      this.from = from;
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      joinExpr.walk(walk);
    }

    @Override
    public String getName() {
      return from.getName();
    }

    @Override
    public void setName(String name) {
      from.setName(name);
      super.setName(name);
    }

    @Override
    public String getAlias() {
      return from.getAlias();
    }

    @Override
    public void setAlias(String alias) {
      from.setAlias(alias);
      super.setAlias(alias);
    }
  }

  public enum DirectionalJoinOp {
    LEFT,
    RIGHT,
    FULL
  }

  public enum CoreJoinOp {
    INNER,
    OUTER,
    CROSS
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public abstract static class Column extends Datum {
    private String name;
    private String relName;

    @SuppressWarnings("unused")
    public Column() {}

    public Column(String relName, String name) {
      this.name = name;
      this.relName = relName;
    }

    public abstract Column rename(String newName);

    public abstract Column withAlias(String alias);
  }

  /**
   * Represents *, i.e. every column in a relation. If an alias is provided we expect the relation
   * to have a single column
   */
  public static class AllColumn extends Column {

    @SuppressWarnings("unused")
    public AllColumn(String relName, String name) {
      super(relName, null);
    }

    @Override
    public Column rename(String newName) {
      throw new RuntimeException("cannot rename *");
    }

    @Override
    public Column withAlias(String alias) {
      throw new RuntimeException("cannot set an alias on *");
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
    }

    @Override
    public String getAlias() {
      return null;
    }

    @Override
    public void setAlias(String alias) {
      this.alias = alias;
    }

    @Override
    public String toString() {
      return "AllColumn{" + "relName='" + super.getRelName() + '\'' + '}';
    }

    @Override
    public boolean equals(Object obj) {
      // a null relname means match all relations
      if (obj instanceof AllColumn that) {
        if (that.getRelName() == null) {
          return true;
        }
        if (this.getRelName() == null) {
          return true;
        }

        return that.getRelName().equals(this.getRelName());
      }
      if (obj instanceof Column) {
        if (this.getRelName() == null) return true;

        return ((Column) obj).getRelName().equals(getRelName());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return getRelName().hashCode();
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class SimpleColumn extends Column {
    public SimpleColumn(String rel, String column) {
      super(rel, column);
    }

    @Override
    public Column rename(String newName) {
      return new SimpleColumn(getRelName(), newName);
    }

    @Override
    public Column withAlias(String alias) {
      return new SimpleColumn(alias, getName());
    }

    public String getNameOrAlias() {
      return Strings.isNullOrEmpty(alias) ? getName() : alias;
    }

    @Override
    public String toString() {
      return "SimpleColumn{"
          + "name='"
          + super.name
          + '\''
          + ", relName='"
          + super.relName
          + '\''
          + ", alias='"
          + super.alias
          + '\''
          + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public abstract static class Expr extends Datum {
    String name;

    @SuppressWarnings("unused")
    public Expr(String name) {
      this.name = name;
    }

    public Expr() {}

    public String getNameOrAlias() {
      return hasAlias() ? getAlias() : getName();
    }

    @Override
    public String toString() {
      return "Expr{" + "name='" + name + '\'' + ", alias='" + super.alias + '\'' + '}';
    }
  }

  public abstract static class BooleanExpr extends Expr {}

  @EqualsAndHashCode(callSuper = true)
  @Data
  @AllArgsConstructor
  public static class WhenExpr extends Expr {

    private Datum when;
    private Datum result;

    @Override
    public void walk(Consumer<Datum> walk) {
      when.walk(walk);
      result.walk(walk);
    }
  }

  @EqualsAndHashCode(callSuper = true)
  @Data
  @AllArgsConstructor
  public static class WhenElseExpr extends Expr {

    private Datum result;

    public WhenElseExpr(String name, Datum result) {
      super(name);
      this.result = result;
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      if (result != null) result.walk(walk);
    }
  }

  @EqualsAndHashCode(callSuper = true)
  @Data
  @AllArgsConstructor
  public static class CaseWhenExpr extends BooleanExpr {

    private List<WhenExpr> whenExprs;
    private WhenElseExpr elseExpr;

    @Override
    public void walk(Consumer<Datum> walk) {
      whenExprs.forEach(e -> e.walk(walk));
      if (elseExpr != null) elseExpr.walk(walk);
    }
  }

  @Data
  public static class BinaryBooleanExpr extends BooleanExpr {
    private Datum left;
    private Datum right;
    private BinaryBooleanOperator op;

    public BinaryBooleanExpr(Datum left, Datum right, BinaryBooleanOperator op) {
      this.left = left;
      this.right = right;
      this.op = op;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BinaryBooleanExpr that = (BinaryBooleanExpr) o;
      var v1 = Objects.equals(left, that.left);
      var v2 = Objects.equals(right, that.right);
      var v3 = op == that.op;
      return v1 && v2 && v3;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), left, right, op);
    }

    @Override
    public String toString() {
      return "BinaryBooleanExpr{"
          + "left="
          + left
          + ", right="
          + right
          + ", op="
          + op
          + ", name='"
          + super.name
          + '\''
          + ", alias='"
          + super.alias
          + '\''
          + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      left.walk(walk);
      right.walk(walk);
    }
  }

  @SuppressWarnings("method")
  public enum BinaryBooleanOperator {
    GREATER_THAN(">"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN_OR_EQUAL(">="),
    EQUAL("="),
    NOT_EQUAL("!="),
    AND("AND"),
    OR("OR"),
    LIKE("LIKE"),
    NOT_LIKE("NOT LIKE"),
    ILIKE("ILIKE"),
    NOT_ILIKE("NOT ILIKE"),
    IN("IN"),
    NOT_IN("NOT IN"),
    BETWEEN("BETWEEN"),
    NOT_BETWEEN("NOT BETWEEN");

    final String op;

    BinaryBooleanOperator(String op) {
      this.op = op;
    }

    @SuppressWarnings("unused")
    public String getOp() {
      return op;
    }

    @Override
    public String toString() {
      return op;
    }

    public static BinaryBooleanOperator fromString(String str) {
      str = str.trim();
      for (var v : values()) {
        if (v.op.equalsIgnoreCase(str)) return v;
      }
      throw new RuntimeException(str + " is not a supported binary boolean operator");
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class UnaryBooleanExpr extends BooleanExpr {
    private Datum expr;
    private UnaryBooleanOperator op;

    public UnaryBooleanExpr(Datum expr, UnaryBooleanOperator op) {
      this.expr = expr;
      this.op = op;
    }

    @Override
    public String toString() {
      return "UnaryBooleanExpr{"
          + "expr="
          + expr
          + ", op="
          + op
          + ", name='"
          + super.name
          + '\''
          + ", alias='"
          + super.alias
          + '\''
          + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      expr.walk(walk);
    }
  }

  public enum UnaryBooleanOperator {
    NOT("NOT"),
    IS_NULL("IS NULL"),
    IS_NOT_NULL("IS NOT NULL");

    final String op;

    UnaryBooleanOperator(String op) {
      this.op = op;
    }

    @Override
    public String toString() {
      return op;
    }

    public static UnaryBooleanOperator fromString(String str) {
      str = str.trim();
      for (var v : values()) {
        if (v.op.equalsIgnoreCase(str)) return v;
      }
      throw new RuntimeException(str + " is not a supported unary boolean operator");
    }
  }

  @Data
  public static class FuncCall extends Expr {
    private String functionName;
    private List<Datum> args = List.of();

    public FuncCall(String functionName, List<Datum> args) {
      this(functionName);
      this.args = args;
    }

    public FuncCall(String functionName) {
      super();
      this.functionName = functionName;
    }

    @Override
    public String toString() {
      return "FuncCall{"
          + "functionName='"
          + functionName
          + '\''
          + ", args="
          + args
          + ", name='"
          + super.name
          + '\''
          + ", alias='"
          + super.alias
          + '\''
          + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      args.forEach(f -> f.walk(walk));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FuncCall funcCall = (FuncCall) o;
      var v2 = Objects.equals(args, funcCall.args);
      return functionName.equalsIgnoreCase(funcCall.functionName) && v2;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), functionName, args);
    }
  }

  /**
   * Represents a function that returns a set for each call and that is part of a lateral join. When
   * a set returning function is present in the select clause we do: For example: select
   * fn(args)->'a' from tbl where fn is a set returning function We move it to the lateral from
   * clauses, give it a relational name, and a single column schema Like so: select rel_1.col_1->'1'
   * from tbl lateral join fn(args) as rel_1 Args can be any expression and can reference any field
   * from tbl.
   */
  @Getter
  @Setter
  @EqualsAndHashCode(callSuper = false)
  public static class LateralFuncFrom extends From {

    private FuncCall funcCall;
    private List<Column> schema;

    public LateralFuncFrom(FuncCall funcCall, String relName, List<Column> schema) {
      super(relName);
      // check that the schema relations are equal to the relName
      schema.forEach(
          c -> {
            if (!relName.equals(c.getRelName()))
              throw new RuntimeException(
                  "column "
                      + c.getName()
                      + " relation name "
                      + c.getRelName()
                      + " is not equal to "
                      + relName);
          });
      this.funcCall = funcCall;
      this.schema = schema;
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      funcCall.walk(walk);
      schema.forEach(col -> col.walk(walk));
    }

    @Override
    public String toString() {
      return "LateralFuncFrom{"
          + "funcCall="
          + funcCall
          + ", schema="
          + schema
          + ", name='"
          + getName()
          + '\''
          + '}';
    }
  }

  @Data
  public static class BinaryArithmeticExpr extends Expr {
    private Datum left;
    private Datum right;
    private BinaryArithmeticOperator op;

    public BinaryArithmeticExpr(Datum left, Datum right, BinaryArithmeticOperator op) {
      super();
      this.left = left;
      this.right = right;
      this.op = op;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      BinaryArithmeticExpr that = (BinaryArithmeticExpr) o;
      return Objects.equals(left, that.left) && Objects.equals(right, that.right) && op == that.op;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), left, right, op);
    }

    @Override
    public String toString() {
      return "BinaryArithmeticExpr{"
          + "left="
          + left
          + ", right="
          + right
          + ", op="
          + op
          + ", name='"
          + super.name
          + '\''
          + ", alias='"
          + super.alias
          + '\''
          + '}';
    }

    @Override
    public void walk(Consumer<Datum> walk) {
      walk.accept(this);
      left.walk(walk);
      right.walk(walk);
    }
  }

  public enum BinaryArithmeticOperator {
    MULTIPLY("*"),
    DIVIDE("/"),
    ADD("+"),
    SUBTRACT("-"),
    MODULUS("%"),
    POWER("^"),
    STRING_CONCAT("||");

    final String op;

    BinaryArithmeticOperator(String op) {
      this.op = op;
    }

    public static BinaryArithmeticOperator fromString(String str) {
      str = str.trim();
      for (var v : values()) {
        if (v.op.equalsIgnoreCase(str)) return v;
      }
      throw new RuntimeException(str + " is not a supported arithmetic operator");
    }

    @Override
    public String toString() {
      return op;
    }
  }
}
