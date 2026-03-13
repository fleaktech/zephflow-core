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
package io.fleak.zephflow.lib.sql.exec.types;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;

public abstract class TypeCast<T> {

  public abstract T cast(Object v);

  public static class LongTypeCast extends TypeCast<Long> {
    @Override
    public Long cast(Object v) {
      return switch (v) {
        case null -> 0L;
        case Long l -> l;
        case Number n -> n.longValue();
        case Boolean b -> b ? 1L : 0L;
        case String s -> Long.parseLong(s);
        default ->
            throw new RuntimeException(v + " type " + v.getClass() + " cannot be cast to long");
      };
    }
  }

  public static class IntegerTypeCast extends TypeCast<Integer> {
    @Override
    public Integer cast(Object v) {
      return switch (v) {
        case null -> 0;
        case Integer i -> i;
        case Number n -> n.intValue();
        case Boolean b -> b ? 1 : 0;
        case String s -> Integer.parseInt(s);
        default ->
            throw new RuntimeException(v + " type " + v.getClass() + " cannot be cast to integer");
      };
    }
  }

  public static class ShortTypeCast extends TypeCast<Short> {
    @Override
    public Short cast(Object v) {
      return switch (v) {
        case null -> 0;
        case Short s -> s;
        case Number n -> n.shortValue();
        case Boolean b -> (short) (b ? 1 : 0);
        case String s -> Short.parseShort(s);
        default ->
            throw new RuntimeException(v + " type " + v.getClass() + " cannot be cast to short");
      };
    }
  }

  public static class DoubleTypeCast extends TypeCast<Double> {
    @Override
    public Double cast(Object v) {
      return switch (v) {
        case null -> 0.0D;
        case Double d -> d;
        case NumberPrimitiveFleakData n -> n.getNumberValue();
        case StringPrimitiveFleakData s -> Double.parseDouble(s.getStringValue());
        case Number n -> n.doubleValue();
        case String s -> Double.parseDouble(s);
        default ->
            throw new ClassCastException(v + " type " + v.getClass() + " cannot be cast to double");
      };
    }
  }

  public static class BigDecimalTypeCast extends TypeCast<BigDecimal> {
    @Override
    public BigDecimal cast(Object v) {
      return switch (v) {
        case null -> BigDecimal.ZERO;
        case Double d -> BigDecimal.valueOf(d);
        case Number n -> BigDecimal.valueOf(n.longValue());
        default -> new BigDecimal(v.toString());
      };
    }
  }

  public static class BooleanTypeCast extends TypeCast<Boolean> {
    @Override
    public Boolean cast(Object v) {
      return switch (v) {
        case null -> Boolean.FALSE;
        case Float f ->
            throw new RuntimeException("Cannot cast " + v.getClass() + " to a boolean type");
        case Double d ->
            throw new RuntimeException("Cannot cast " + v.getClass() + " to a boolean type");
        case Number n -> Math.abs(n.intValue()) > 0 ? Boolean.TRUE : Boolean.FALSE;
        case Boolean b -> b;
        case String s ->
            switch (s.toLowerCase()) {
              case "true", "t", "yes", "y", "1", "on" -> true;
              case "false", "f", "no", "n", "0", "off" -> false;
              default ->
                  throw new RuntimeException("Cannot cast " + v.getClass() + " to a boolean type");
            };
        default -> throw new RuntimeException("Cannot cast " + v.getClass() + " to a boolean type");
      };
    }
  }

  public static class StringTypeCast extends TypeCast<String> {
    @Override
    public String cast(Object v) {
      return switch (v) {
        case null -> null;
        case String s -> s;
        case Number n -> v.toString();
        case Boolean b -> v.toString();
        default -> JsonUtils.toJsonString(v);
      };
    }
  }

  @SuppressWarnings({"all"})
  public static class MapTypeCast extends TypeCast<Map<?, ?>> {

    @Override
    public Map<?, ?> cast(Object v) {
      return switch (v) {
        case null -> null;
        case Map m -> m;
        case String s -> {
          try {
            yield OBJECT_MAPPER.readValue(s, Map.class);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        }
        case ObjectNode on -> OBJECT_MAPPER.convertValue(on, Map.class);
        default -> throw new ClassCastException("cannot cast " + v.getClass() + " to a map");
      };
    }
  }

  @SuppressWarnings({"all"})
  public static class ListTypeCast extends TypeCast<List<?>> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public List<?> cast(Object v) {
      if (v == null) return List.of();

      if (v.getClass().isArray()) {
        return createList(v);
      }

      return switch (v) {
        case Map m -> m.keySet().stream().toList();
        case List<?> l -> l;
        case String s -> {
          try {
            yield MAPPER.readValue(s, List.class);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        }
        case Collection<?> c -> new ArrayList<>(c);
        case ArrayNode an -> {
          var jsonNodeArrayList = new ArrayList<JsonNode>();
          an.forEach(jsonNodeArrayList::add);
          yield jsonNodeArrayList;
        }
        default -> throw new ClassCastException("cannot cast " + v.getClass() + " to a list");
      };
    }

    /** Use reflection to convert Object and Primitive arrays to a list */
    private static <T> List<T> createList(Object arr) {
      int len = Array.getLength(arr);
      var list = new ArrayList<T>(len);

      for (int i = 0; i < len; i++) {
        list.add((T) Array.get(arr, i));
      }
      return list;
    }
  }

  @SuppressWarnings("unchecked")
  public static class IterableTypeCast extends TypeCast<Iterable<?>> {
    @Override
    public Iterable<?> cast(Object v) {
      if (v.getClass().isArray()) {
        return createList(v);
      }

      return switch (v) {
        case Map<?, ?> m -> m.keySet().stream().toList();
        case Iterable<?> l -> l;
        default -> throw new ClassCastException("cannot cast " + v.getClass() + " to a iterable");
      };
    }

    /** Use reflection to convert Object and Primitive arrays to a list */
    private static <T> List<T> createList(Object arr) {
      int len = Array.getLength(arr);
      var list = new ArrayList<T>(len);

      for (int i = 0; i < len; i++) {
        list.add((T) Array.get(arr, i));
      }
      return list;
    }
  }

  public static class NoopTypeCast extends TypeCast<Object> {
    @Override
    public Object cast(Object v) {
      return v;
    }
  }

  public static class JsonTypeCast extends TypeCast<JsonNode> {

    @Override
    public JsonNode cast(Object v) {
      if (v == null) return null;
      try {
        return OBJECT_MAPPER.readTree(v.toString());
      } catch (JsonProcessingException e) {
        throw new ClassCastException("cannot cast " + v + " to a JsonNode");
      }
    }
  }
}
