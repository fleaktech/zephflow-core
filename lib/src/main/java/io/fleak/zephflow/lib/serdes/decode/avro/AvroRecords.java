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
package io.fleak.zephflow.lib.serdes.decode.avro;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

/**
 * Avro values to records, following the schema. Timestamps become epoch milliseconds (finer units
 * rounded down), local timestamps are read as UTC, dates are days since 1970-01-01, times of day
 * are milliseconds, decimals are exact strings, uuids are strings, other bytes and fixed are
 * base64, and null record fields are left out (null array items and map values stay). Non-finite
 * floating point values become strings, since JSON has no such numbers.
 */
final class AvroRecords {

  private AvroRecords() {}

  static RecordFleakData toRecord(IndexedRecord record) {
    return (RecordFleakData) convert(record, record.getSchema());
  }

  private static FleakData convert(Object value, Schema schema) {
    if (value == null) {
      return null;
    }
    LogicalType logical = schema.getLogicalType();
    return switch (schema.getType()) {
      case NULL -> null;
      case BOOLEAN -> new BooleanPrimitiveFleakData((Boolean) value);
      case INT -> number(((Integer) value).longValue());
      case LONG -> number(scaleTime(logical, (Long) value));
      case FLOAT -> real(((Float) value).doubleValue());
      case DOUBLE -> real((Double) value);
      case STRING, ENUM -> new StringPrimitiveFleakData(value.toString());
      case BYTES -> bytes(logical, toArray((ByteBuffer) value));
      case FIXED -> fixed(logical, ((GenericFixed) value).bytes());
      case ARRAY -> {
        List<FleakData> items = new ArrayList<>();
        for (Object item : (Collection<?>) value) {
          items.add(convert(item, schema.getElementType()));
        }
        yield new ArrayFleakData(items);
      }
      case MAP -> {
        Map<String, FleakData> entries = new HashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          entries.put(entry.getKey().toString(), convert(entry.getValue(), schema.getValueType()));
        }
        yield new RecordFleakData(entries);
      }
      case UNION -> {
        int branch = GenericData.get().resolveUnion(schema, value);
        yield convert(value, schema.getTypes().get(branch));
      }
      case RECORD -> {
        IndexedRecord record = (IndexedRecord) value;
        Map<String, FleakData> fields = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {
          FleakData converted = convert(record.get(field.pos()), field.schema());
          if (converted != null) {
            fields.put(field.name(), converted);
          }
        }
        yield new RecordFleakData(fields);
      }
    };
  }

  /** Microsecond and nanosecond times and timestamps to milliseconds, rounded down. */
  private static long scaleTime(LogicalType logical, long value) {
    if (logical instanceof LogicalTypes.TimeMicros
        || logical instanceof LogicalTypes.TimestampMicros
        || logical instanceof LogicalTypes.LocalTimestampMicros) {
      return Math.floorDiv(value, 1_000L);
    }
    if (logical instanceof LogicalTypes.TimestampNanos
        || logical instanceof LogicalTypes.LocalTimestampNanos) {
      return Math.floorDiv(value, 1_000_000L);
    }
    return value;
  }

  private static FleakData bytes(LogicalType logical, byte[] bytes) {
    if (logical instanceof LogicalTypes.Decimal decimal) {
      return new StringPrimitiveFleakData(
          new BigDecimal(new BigInteger(bytes), decimal.getScale()).toPlainString());
    }
    return new StringPrimitiveFleakData(Base64.getEncoder().encodeToString(bytes));
  }

  /** A uuid on a 16-byte fixed becomes the same string a uuid on a string would be. */
  private static FleakData fixed(LogicalType logical, byte[] bytes) {
    if (logical != null && "uuid".equals(logical.getName()) && bytes.length == 16) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return new StringPrimitiveFleakData(new UUID(buffer.getLong(), buffer.getLong()).toString());
    }
    return bytes(logical, bytes);
  }

  private static byte[] toArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
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
