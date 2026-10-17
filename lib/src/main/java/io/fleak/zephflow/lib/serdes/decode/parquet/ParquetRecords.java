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
package io.fleak.zephflow.lib.serdes.decode.parquet;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Parquet rows to records, built while parquet assembles them. A row's size is estimated as 8 bytes
 * per value plus the bytes of its strings and binaries; once over {@code maxRecordBytes} the rest
 * of the row is still read but not built. A value that does not convert loses only its row.
 * Optional fields that are null are left out, null list items and map values stay null, a LIST or
 * MAP group follows the parquet spec's compatibility rules, and an unannotated repeated field is an
 * array that is empty when the field never occurs.
 */
final class ParquetRecords extends RecordMaterializer<RecordFleakData> {

  private static final long VALUE_BYTES = 8;
  private static final long UNIX_EPOCH_JULIAN_DAY = 2_440_588L;

  private final long maxRecordBytes;
  private final GroupConverter root;
  private RecordFleakData current;
  private long size;
  private Exception failure;

  ParquetRecords(MessageType schema, long maxRecordBytes) {
    this.maxRecordBytes = maxRecordBytes;
    RecordConverter record =
        new RecordConverter(schema, value -> current = (RecordFleakData) value);
    this.root =
        new GroupConverter() {
          @Override
          public Converter getConverter(int fieldIndex) {
            return record.getConverter(fieldIndex);
          }

          @Override
          public void start() {
            current = null;
            size = 0;
            failure = null;
            record.start();
          }

          @Override
          public void end() {
            record.end();
          }
        };
  }

  @Override
  public RecordFleakData getCurrentRecord() {
    return current;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }

  /** Estimated bytes of the last row, as far as it was built. */
  long size() {
    return size;
  }

  boolean oversized() {
    return maxRecordBytes > 0 && size > maxRecordBytes;
  }

  /** Why a value of the last row did not convert, or null. */
  Exception failure() {
    return failure;
  }

  /** Counts a value and says whether to build it. */
  private boolean charge(long bytes) {
    size += bytes;
    return !oversized() && failure == null;
  }

  @FunctionalInterface
  private interface Sink {
    void accept(FleakData value);
  }

  private Converter converter(Type type, Sink sink) {
    if (type.isPrimitive()) {
      return new ValueConverter(type.asPrimitiveType(), sink);
    }
    GroupType group = type.asGroupType();
    LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
    if (annotation instanceof ListLogicalTypeAnnotation && isWrapper(group)) {
      return new ListConverter(group, sink);
    }
    if ((annotation instanceof MapLogicalTypeAnnotation
            || annotation instanceof MapKeyValueTypeAnnotation)
        && isWrapper(group)
        && !group.getType(0).isPrimitive()
        && group.getType(0).asGroupType().getFieldCount() <= 2) {
      return new MapConverter(group, sink);
    }
    return new RecordConverter(group, sink);
  }

  /** One repeated field, as a LIST or MAP group has. */
  private static boolean isWrapper(GroupType group) {
    return group.getFieldCount() == 1 && group.getType(0).isRepetition(Type.Repetition.REPEATED);
  }

  private final class RecordConverter extends GroupConverter {

    private final String[] names;
    private final boolean[] repeated;
    private final Converter[] fields;
    private final Sink sink;
    private Map<String, FleakData> values;
    private List<List<FleakData>> lists;

    RecordConverter(GroupType type, Sink sink) {
      this.sink = sink;
      int count = type.getFieldCount();
      names = new String[count];
      repeated = new boolean[count];
      fields = new Converter[count];
      for (int i = 0; i < count; i++) {
        Type field = type.getType(i);
        String name = field.getName();
        int index = i;
        names[i] = name;
        repeated[i] = field.isRepetition(Type.Repetition.REPEATED);
        Sink into =
            repeated[i] ? value -> lists.get(index).add(value) : value -> values.put(name, value);
        fields[i] = converter(field, into);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return fields[fieldIndex];
    }

    @Override
    public void start() {
      values = new HashMap<>();
      lists = new ArrayList<>(names.length);
      for (int i = 0; i < names.length; i++) {
        lists.add(repeated[i] ? new ArrayList<>() : null);
      }
    }

    @Override
    public void end() {
      Map<String, FleakData> built = values;
      for (int i = 0; i < names.length; i++) {
        if (repeated[i]) {
          built.put(names[i], new ArrayFleakData(lists.get(i)));
        }
      }
      if (charge(VALUE_BYTES)) {
        sink.accept(new RecordFleakData(built));
      }
    }
  }

  private final class ListConverter extends GroupConverter {

    private final Converter repeated;
    private final Sink sink;
    private List<FleakData> items;

    ListConverter(GroupType list, Sink sink) {
      this.sink = sink;
      Type element = list.getType(0);
      boolean elementIsRepeated =
          element.isPrimitive()
              || element.asGroupType().getFieldCount() != 1
              || element.getName().equals("array")
              || element.getName().equals(list.getName() + "_tuple");
      Sink into = value -> items.add(value);
      repeated =
          elementIsRepeated
              ? converter(element, into)
              : new ElementConverter(element.asGroupType(), into);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return repeated;
    }

    @Override
    public void start() {
      items = new ArrayList<>();
    }

    @Override
    public void end() {
      if (charge(VALUE_BYTES)) {
        sink.accept(new ArrayFleakData(items));
      }
    }
  }

  /** The repeated group of a three-level list; its one field is the item, null when absent. */
  private final class ElementConverter extends GroupConverter {

    private final Converter element;
    private final Sink sink;
    private FleakData item;

    ElementConverter(GroupType repeated, Sink sink) {
      this.sink = sink;
      this.element = converter(repeated.getType(0), value -> item = value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return element;
    }

    @Override
    public void start() {
      item = null;
    }

    @Override
    public void end() {
      if (charge(VALUE_BYTES)) {
        sink.accept(item);
      }
    }
  }

  private final class MapConverter extends GroupConverter {

    private final Converter entry;
    private final Sink sink;
    private Map<String, FleakData> entries;

    MapConverter(GroupType map, Sink sink) {
      this.sink = sink;
      this.entry = new EntryConverter(map.getType(0).asGroupType());
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return entry;
    }

    @Override
    public void start() {
      entries = new HashMap<>();
    }

    @Override
    public void end() {
      if (charge(VALUE_BYTES)) {
        sink.accept(new RecordFleakData(entries));
      }
    }

    private final class EntryConverter extends GroupConverter {

      private final Converter[] fields;
      private FleakData key;
      private FleakData value;

      EntryConverter(GroupType keyValue) {
        fields = new Converter[keyValue.getFieldCount()];
        fields[0] = converter(keyValue.getType(0), built -> key = built);
        if (fields.length > 1) {
          fields[1] = converter(keyValue.getType(1), built -> value = built);
        }
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        return fields[fieldIndex];
      }

      @Override
      public void start() {
        key = null;
        value = null;
      }

      @Override
      public void end() {
        if (key != null && charge(VALUE_BYTES)) {
          entries.put(mapKey(key), value);
        }
      }
    }
  }

  private static String mapKey(FleakData key) {
    return switch (key) {
      case NumberPrimitiveFleakData n when n.getNumberType() == NumberType.LONG ->
          Long.toString((long) n.getNumberValue());
      case NumberPrimitiveFleakData n -> Double.toString(n.getNumberValue());
      default -> String.valueOf(key.unwrap());
    };
  }

  private final class ValueConverter extends PrimitiveConverter {

    private final PrimitiveType type;
    private final LogicalTypeAnnotation annotation;
    private final Sink sink;

    ValueConverter(PrimitiveType type, Sink sink) {
      this.type = type;
      this.annotation = type.getLogicalTypeAnnotation();
      this.sink = sink;
    }

    private void emit(long bytes, ValueBuilder builder) {
      if (!charge(bytes)) {
        return;
      }
      try {
        sink.accept(builder.build());
      } catch (RuntimeException e) {
        failure =
            new MalformedParquetException(
                "column " + type.getName() + " does not convert: " + e.getMessage(), e);
      }
    }

    @Override
    public void addBoolean(boolean value) {
      emit(VALUE_BYTES, () -> new BooleanPrimitiveFleakData(value));
    }

    @Override
    public void addInt(int value) {
      emit(
          VALUE_BYTES,
          () ->
              switch (annotation) {
                case IntLogicalTypeAnnotation i when !i.isSigned() ->
                    number(Integer.toUnsignedLong(value));
                case DecimalLogicalTypeAnnotation d -> decimal(BigInteger.valueOf(value), d);
                case null, default -> number(value);
              });
    }

    @Override
    public void addLong(long value) {
      emit(
          VALUE_BYTES,
          () ->
              switch (annotation) {
                case IntLogicalTypeAnnotation i when !i.isSigned() && value < 0 ->
                    new NumberPrimitiveFleakData(
                        (double) (value >>> 1) * 2.0 + (value & 1), NumberType.DOUBLE);
                case DecimalLogicalTypeAnnotation d -> decimal(BigInteger.valueOf(value), d);
                case TimestampLogicalTypeAnnotation t -> number(millis(value, t.getUnit()));
                case TimeLogicalTypeAnnotation t -> number(millis(value, t.getUnit()));
                case null, default -> number(value);
              });
    }

    @Override
    public void addFloat(float value) {
      emit(VALUE_BYTES, () -> real(value));
    }

    @Override
    public void addDouble(double value) {
      emit(VALUE_BYTES, () -> real(value));
    }

    @Override
    public void addBinary(Binary value) {
      emit(VALUE_BYTES + value.length(), () -> binary(value));
    }

    private FleakData binary(Binary value) {
      if (type.getPrimitiveTypeName() == PrimitiveTypeName.INT96) {
        return number(int96Millis(value));
      }
      return switch (annotation) {
        case StringLogicalTypeAnnotation ignored -> string(value);
        case EnumLogicalTypeAnnotation ignored -> string(value);
        case JsonLogicalTypeAnnotation ignored -> string(value);
        case UUIDLogicalTypeAnnotation ignored -> {
          ByteBuffer bytes = value.toByteBuffer();
          yield new StringPrimitiveFleakData(new UUID(bytes.getLong(), bytes.getLong()).toString());
        }
        case DecimalLogicalTypeAnnotation d -> decimal(new BigInteger(value.getBytes()), d);
        case null, default ->
            new StringPrimitiveFleakData(Base64.getEncoder().encodeToString(value.getBytes()));
      };
    }
  }

  @FunctionalInterface
  private interface ValueBuilder {
    FleakData build();
  }

  private static long millis(long value, TimeUnit unit) {
    return switch (unit) {
      case MILLIS -> value;
      case MICROS -> Math.floorDiv(value, 1_000L);
      case NANOS -> Math.floorDiv(value, 1_000_000L);
    };
  }

  /** Nanoseconds of the day, then the Julian day, both little-endian. */
  private static long int96Millis(Binary value) {
    ByteBuffer bytes = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
    long nanos = bytes.getLong();
    long julianDay = Integer.toUnsignedLong(bytes.getInt());
    return Math.addExact(
        Math.multiplyExact(julianDay - UNIX_EPOCH_JULIAN_DAY, 86_400_000L),
        Math.floorDiv(nanos, 1_000_000L));
  }

  private static FleakData decimal(BigInteger unscaled, DecimalLogicalTypeAnnotation decimal) {
    return new StringPrimitiveFleakData(
        new BigDecimal(unscaled, decimal.getScale()).toPlainString());
  }

  private static FleakData string(Binary value) {
    return new StringPrimitiveFleakData(new String(value.getBytes(), StandardCharsets.UTF_8));
  }

  private static FleakData number(long value) {
    return new NumberPrimitiveFleakData(value, NumberType.LONG);
  }

  private static FleakData real(double value) {
    return Double.isFinite(value)
        ? new NumberPrimitiveFleakData(value, NumberType.DOUBLE)
        : new StringPrimitiveFleakData(Double.toString(value));
  }
}
