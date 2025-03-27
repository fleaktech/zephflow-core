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
      if (v == null) return 0L;

      if (v instanceof Long) return (Long) v;

      if (v instanceof Number) return ((Number) v).longValue();

      if (v instanceof Boolean) return (Boolean) v ? 1L : 0L;

      if (v instanceof String) return Long.parseLong((String) v);

      throw new RuntimeException(v + " type " + v.getClass() + " cannot be cast to long");
    }
  }

  public static class IntegerTypeCast extends TypeCast<Integer> {
    @Override
    public Integer cast(Object v) {
      if (v == null) return 0;

      if (v instanceof Integer) return (Integer) v;

      if (v instanceof Number) return ((Number) v).intValue();

      if (v instanceof Boolean) return (Boolean) v ? 1 : 0;

      if (v instanceof String) return Integer.parseInt((String) v);

      throw new RuntimeException(v + " type " + v.getClass() + " cannot be cast to integer");
    }
  }

  public static class ShortTypeCast extends TypeCast<Short> {
    @Override
    public Short cast(Object v) {
      if (v == null) return 0;

      if (v instanceof Short) return (Short) v;

      if (v instanceof Number) return ((Number) v).shortValue();

      if (v instanceof Boolean) return (short) ((Boolean) v ? 1 : 0);

      if (v instanceof String) return Short.parseShort((String) v);

      throw new RuntimeException(v + " type " + v.getClass() + " cannot be cast to short");
    }
  }

  public static class DoubleTypeCast extends TypeCast<Double> {
    @Override
    public Double cast(Object v) {
      if (v == null) return 0.0D;

      if (v instanceof Double) return (Double) v;

      if (v instanceof Number) return ((Number) v).doubleValue();

      if (v instanceof String) return Double.parseDouble((String) v);

      if (v instanceof NumberPrimitiveFleakData)
        return ((NumberPrimitiveFleakData) v).getNumberValue();

      if (v instanceof StringPrimitiveFleakData)
        return Double.parseDouble(((StringPrimitiveFleakData) v).getStringValue());

      throw new ClassCastException(v + " type " + v.getClass() + " cannot be cast to double");
    }
  }

  public static class BigDecimalTypeCast extends TypeCast<BigDecimal> {
    @Override
    public BigDecimal cast(Object v) {
      if (v == null) return BigDecimal.ZERO;

      if (v instanceof Double) {
        return BigDecimal.valueOf((Double) v);
      }

      if (v instanceof Number) {
        return BigDecimal.valueOf(((Number) v).longValue());
      }

      return new BigDecimal(v.toString());
    }
  }

  public static class BooleanTypeCast extends TypeCast<Boolean> {
    @Override
    public Boolean cast(Object v) {
      if (v == null) return Boolean.FALSE;

      if (v instanceof Number n && !(v instanceof Float || v instanceof Double))
        return Math.abs(n.intValue()) > 0 ? Boolean.TRUE : Boolean.FALSE;

      if (v instanceof Boolean b) return b;

      if (v instanceof String s) {
        switch (s.toLowerCase()) {
          case "true":
          case "t":
          case "yes":
          case "y":
          case "1":
          case "on":
            return true;
          case "false":
          case "f":
          case "no":
          case "n":
          case "0":
          case "off":
            return false;
        }
      }

      throw new RuntimeException("Cannot cast " + v.getClass() + " to a boolean type");
    }
  }

  public static class StringTypeCast extends TypeCast<String> {
    @Override
    public String cast(Object v) {

      if (v == null) return null;

      if (v instanceof String s) {
        return s;
      }

      if (v instanceof Number || v instanceof Boolean) {
        return v.toString();
      }

      return JsonUtils.toJsonString(v);
    }
  }

  @SuppressWarnings({"all"})
  public static class MapTypeCast extends TypeCast<Map<?, ?>> {

    @Override
    public Map<?, ?> cast(Object v) {
      if (v == null) return null;

      if (v instanceof Map m) {
        return m;
      }
      if (v instanceof String s) {
        try {
          return OBJECT_MAPPER.readValue(s, Map.class);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      if (v instanceof ObjectNode on) {
        return OBJECT_MAPPER.convertValue(on, Map.class);
      }
      throw new ClassCastException("cannot cast " + v.getClass() + " to a map");
    }
  }

  @SuppressWarnings({"all"})
  public static class ListTypeCast extends TypeCast<List<?>> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public List<?> cast(Object v) {
      if (v == null) return List.of();

      if (v instanceof Map m) {
        return m.keySet().stream().toList();
      }
      if (v instanceof List<?> l) {
        return l;
      }
      if (v.getClass().isArray()) {
        return createList(v);
      }
      if (v instanceof String s) {
        try {
          return MAPPER.readValue(s, List.class);
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      if (v instanceof Collection<?> c) {
        return new ArrayList<>(c);
      }
      if (v instanceof ArrayNode an) {
        ArrayList<JsonNode> jsonNodeArrayList = new ArrayList<>();
        an.forEach(jn -> jsonNodeArrayList.add(jn));
        return jsonNodeArrayList;
      }

      throw new ClassCastException("cannot cast " + v.getClass() + " to a list");
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

      if (v instanceof Map<?, ?> m) {
        return m.keySet().stream().toList();
      } else if (v instanceof Iterable<?> l) {
        return l;
      } else if (v.getClass().isArray()) {
        return createList(v);
      }

      throw new ClassCastException("cannot cast " + v.getClass() + " to a iterable");
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
