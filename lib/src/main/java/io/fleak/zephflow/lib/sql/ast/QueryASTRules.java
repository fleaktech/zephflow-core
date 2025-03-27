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
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class QueryASTRules {

  public static QueryAST.Query applyTypeRules(QueryAST.Query query, TypeSystem typeSystem) {
    query.walk(
        (datum -> {
          if (datum instanceof QueryAST.FuncCall f) {
            // exception is thrown by the lookup function if the function does not exist
            typeSystem.lookupFunction(f.getFunctionName());
          }
        }));

    return query;
  }

  public static QueryAST.Query apply(QueryAST.Query query) {

    // qualify all aggregate functions
    if (!query.getAggregateFunctions().isEmpty()) validateAggregateFunctions(query);

    fullyQualifyIdentifierNames(query);
    if (!query.getGroupBy().isEmpty()) validateSelectUsesGroupBy(query);

    addImplicitGroupBy(query);

    return query;
  }

  /** Support queries like select count(*) from table */
  private static void addImplicitGroupBy(QueryAST.Query query) {
    if (query.getGroupBy().isEmpty() && !query.getAggregateFunctions().isEmpty()) {
      // we are using an aggregate function
      // a group by 1 will cause all rows to group under the same key.
      query.setGroupBy(List.of(QueryAST.constant(1L)));
    }
  }

  private static void validateAggregateFunctions(QueryAST.Query query) {
    if (query.getFrom().isEmpty()) {
      throw new RuntimeException("the query must specify a from table");
    }
  }

  private static void validateSelectUsesGroupBy(QueryAST.Query query) {
    Set<String> groupByLookup = new HashSet<>();
    for (var datum : query.getGroupBy()) {
      if (datum instanceof QueryAST.SimpleColumn c) {
        groupByLookup.add(c.getRelName() + "." + c.getName());
      }
    }

    for (var datum : query.getColumns()) {
      if (datum instanceof QueryAST.SimpleColumn c) {
        // skip internal columns, these are introduced to replaced the aggregate function in the
        // projection part
        if (c.getName().startsWith("col_")) continue;
        // use the name, the alias is for after the select is done
        var fqn = c.getRelName() + "." + c.getName();
        if (!groupByLookup.contains(fqn)) {
          throw new RuntimeException(fqn + " must be in the group by clause");
        }
      }
    }
  }

  /** Fully qualify all columns and ensure that they have a name */
  public static void fullyQualifyIdentifierNames(QueryAST.Query query) {

    AtomicReference<Set<String>> parentRels = new AtomicReference<>();
    AtomicReference<Set<String>> localRels = new AtomicReference<>(Set.of());
    AtomicReference<Set<String>> lateralRels = new AtomicReference<>(Set.of());

    AtomicInteger counter = new AtomicInteger(0);

    query.walk(
        (datum) -> {
          if (datum instanceof QueryAST.Query q) {
            parentRels.set(localRels.get());
            localRels.set(validateFromDatums(q));

            lateralRels.set(
                q.getLateralFroms().stream()
                    .map(QueryAST.From::getAliasOrName)
                    .collect(Collectors.toSet()));

          } else if (datum instanceof QueryAST.Column c) {
            validateColumn(
                datum, Optional.ofNullable(parentRels.get()), localRels.get(), lateralRels.get());
            if (Strings.isNullOrEmpty(c.getName())) {
              c.setName("col_" + counter.incrementAndGet());
            }
          } else if (datum instanceof QueryAST.Expr e) {
            // all expressions must have a name or alias
            if (Strings.isNullOrEmpty(e.getName())) {
              e.setName("col_" + counter.incrementAndGet());
            }
          }
        });
  }

  /**
   * For each from, if its a SimpleFrom or a SubSelectFrom we get the relation name from the alias
   * or name property. We check if it is a duplicate name and if so we raise an error.
   *
   * <p>Return the set of relation names for the query.
   */
  private static Set<String> validateFromDatums(QueryAST.Query query) {
    Set<String> localRels = new HashSet<>();

    // Do not include lateral froms here, columns that use lateral forms are already resolved in the
    // listener
    query
        .getFrom()
        .forEach(
            from -> {
              if (Strings.isNullOrEmpty(from.getName()) && Strings.isNullOrEmpty(from.getAlias())) {
                throw new RuntimeException(
                    "from clause must have an alias or name defined: " + from);
              }

              var relName = Strings.isNullOrEmpty(from.alias) ? from.getName() : from.alias;
              if (localRels.contains(relName)) {
                throw new RuntimeException(
                    "alias or name was already specified in the from clause; " + relName);
              }

              localRels.add(relName);
            });

    return localRels;
  }

  /**
   * Check if: * the column is not fully qualified e.g select a from input, then set input.a, if we
   * have select a from input1, input2 then we raise an exception * if we have select input1.a,
   * input2.b from input1, input2 we check that input1 and input2 is in the from section
   */
  @SuppressWarnings("all")
  private static void validateColumn(
      QueryAST.Datum col,
      Optional<Set<String>> parentRels,
      Set<String> localRels,
      Set<String> lateralRels) {
    if (col instanceof QueryAST.SimpleColumn simpleCol) {
      var relName = simpleCol.getRelName();

      // no relation specified either fully qualify or raise an ambiguous error
      // we force fqn names when using more than one table
      if (Strings.isNullOrEmpty(relName)) {
        // we allow a column to directly refer the alias of a lateral join
        if (lateralRels.contains(simpleCol.getName())) {
          // if the column refers to a lateral, we set the relation name to that of the lateral
          simpleCol.setRelName(simpleCol.getName());
        } else {
          if (localRels.size() > 1) {
            throw new RuntimeException(
                "ambiguous column "
                    + simpleCol.getName()
                    + " please prefix with one of "
                    + String.join(",", localRels));
          }

          if (localRels.isEmpty()) {
            throw new RuntimeException("no relation in the from clause");
          }

          simpleCol.setRelName(localRels.stream().findFirst().orElseThrow());
        }
      } else {
        // we have a rel name, see if it exists in the local the parent rels
        if (!(lateralRels.contains(relName)
            || localRels.contains(relName)
            || parentRels.orElseGet(Set::of).contains(relName))) {
          throw new RuntimeException(
              "ambiguous or uknown column "
                  + simpleCol.getRelName()
                  + "."
                  + simpleCol.getName()
                  + " please prefix with one of "
                  + String.join(",", parentRels.orElseGet(Set::of))
                  + " or "
                  + String.join(",", localRels));
        }
      }
    }
  }
}
