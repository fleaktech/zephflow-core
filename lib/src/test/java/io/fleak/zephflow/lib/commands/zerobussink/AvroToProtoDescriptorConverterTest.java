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
package io.fleak.zephflow.lib.commands.zerobussink;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

class AvroToProtoDescriptorConverterTest {

  private static Map<String, Object> field(String name, Object type) {
    return Map.of("name", name, "type", type);
  }

  private static Map<String, Object> recordSchema(List<Map<String, Object>> fields) {
    return Map.of("type", "record", "name", "TestRecord", "fields", fields);
  }

  @Test
  void buildsDescriptorForScalarTypes() {
    Map<String, Object> avro =
        recordSchema(
            List.of(
                field("id", "int"),
                field("name", "string"),
                field("score", "double"),
                field("active", "boolean")));

    DescriptorProto proto = AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t");
    Descriptors.Descriptor descriptor = AvroToProtoDescriptorConverter.toDescriptor(proto);

    assertNotNull(descriptor.findFieldByName("id"));
    assertNotNull(descriptor.findFieldByName("name"));
    assertEquals(4, descriptor.getFields().size());
  }

  @Test
  void encodesScalarRecordRoundTrip() throws Exception {
    Map<String, Object> avro = recordSchema(List.of(field("id", "int"), field("name", "string")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("id", 42, "name", "alice"), schema, descriptor);

    // round-trip the wire bytes through the same descriptor
    DynamicMessage parsed = DynamicMessage.parseFrom(descriptor, msg.toByteArray());
    assertEquals(42, parsed.getField(descriptor.findFieldByName("id")));
    assertEquals("alice", parsed.getField(descriptor.findFieldByName("name")));
  }

  @Test
  void nullableUnionFieldIsOptional() {
    Map<String, Object> avro =
        recordSchema(List.of(field("id", "int"), field("maybe", List.of("null", "string"))));
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));
    Descriptors.FieldDescriptor maybe = descriptor.findFieldByName("maybe");
    assertNotNull(maybe);
    assertEquals(Descriptors.FieldDescriptor.Type.STRING, maybe.getType());

    // missing optional field => no value set, encodes cleanly
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("id", 1), schema, descriptor);
    assertFalse(msg.hasField(maybe));
  }

  @Test
  void dateLogicalTypeEncodedAsEpochDay() throws Exception {
    Map<String, Object> dateType = Map.of("type", "int", "logicalType", "date");
    Map<String, Object> avro = recordSchema(List.of(field("d", dateType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("d", "2026-01-15"), schema, descriptor);
    int expected = (int) LocalDate.parse("2026-01-15").toEpochDay();
    assertEquals(expected, msg.getField(descriptor.findFieldByName("d")));
  }

  @Test
  void timestampMicrosFromStringPreservesSubMillis() {
    Map<String, Object> tsType = Map.of("type", "long", "logicalType", "timestamp-micros");
    Map<String, Object> avro = recordSchema(List.of(field("ts", tsType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    // .123456 seconds = 123456 micros; must NOT be truncated to milli precision
    Instant instant = Instant.parse("2026-01-15T10:00:00.123456Z");
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("ts", instant.toString()), schema, descriptor);
    long expectedMicros = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    assertEquals(expectedMicros, msg.getField(descriptor.findFieldByName("ts")));
  }

  @Test
  void timestampMicrosNumericPassesThroughUnchanged() {
    Map<String, Object> tsType = Map.of("type", "long", "logicalType", "timestamp-micros");
    Map<String, Object> avro = recordSchema(List.of(field("ts", tsType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    long micros = 1_736_935_200_123_456L; // already micros
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("ts", micros), schema, descriptor);
    assertEquals(micros, msg.getField(descriptor.findFieldByName("ts")));
  }

  @Test
  void timestampMillisNumericConvertedToMicros() {
    Map<String, Object> tsType = Map.of("type", "long", "logicalType", "timestamp-millis");
    Map<String, Object> avro = recordSchema(List.of(field("ts", tsType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    long millis = 1_736_935_200_123L;
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("ts", millis), schema, descriptor);
    assertEquals(millis * 1000L, msg.getField(descriptor.findFieldByName("ts")));
  }

  @Test
  void repeatedArrayField() {
    Map<String, Object> arrayType = Map.of("type", "array", "items", "string");
    Map<String, Object> avro = recordSchema(List.of(field("tags", arrayType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));
    Descriptors.FieldDescriptor tags = descriptor.findFieldByName("tags");
    assertTrue(tags.isRepeated());

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("tags", List.of("a", "b", "c")), schema, descriptor);
    assertEquals(3, msg.getRepeatedFieldCount(tags));
  }

  @Test
  void nestedRecordField() throws Exception {
    Map<String, Object> nested =
        Map.of(
            "type",
            "record",
            "name",
            "Address",
            "fields",
            List.of(field("city", "string"), field("zip", "string")));
    Map<String, Object> avro = recordSchema(List.of(field("addr", nested)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));
    Descriptors.FieldDescriptor addr = descriptor.findFieldByName("addr");
    assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, addr.getType());

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("addr", Map.of("city", "SF", "zip", "94107")), schema, descriptor);
    DynamicMessage parsed = DynamicMessage.parseFrom(descriptor, msg.toByteArray());
    DynamicMessage addrMsg = (DynamicMessage) parsed.getField(addr);
    assertEquals("SF", addrMsg.getField(addr.getMessageType().findFieldByName("city")));
  }

  @Test
  void mapField() {
    Map<String, Object> mapType = Map.of("type", "map", "values", "string");
    Map<String, Object> avro = recordSchema(List.of(field("labels", mapType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));
    Descriptors.FieldDescriptor labels = descriptor.findFieldByName("labels");
    assertTrue(labels.isMapField());

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("labels", Map.of("env", "prod", "team", "data")), schema, descriptor);
    assertEquals(2, msg.getRepeatedFieldCount(labels));
  }

  @Test
  void decimalTypeRejected() {
    Map<String, Object> decimalType =
        Map.of("type", "bytes", "logicalType", "decimal", "precision", 10, "scale", 2);
    Map<String, Object> avro = recordSchema(List.of(field("amount", decimalType)));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));
    assertTrue(e.getMessage().contains("DECIMAL"));
  }

  @Test
  void nonRecordSchemaRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroToProtoDescriptorConverter.parseAvro(Map.of("type", "string")));
  }

  // --- #3: missing-value handling ---

  @Test
  void missingNonNullableFieldThrows() {
    // id is a plain (non-nullable) int; omitting it must fail loudly, not encode as default 0.
    Map<String, Object> avro = recordSchema(List.of(field("id", "int"), field("name", "string")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("name", "alice"), schema, descriptor));
    assertTrue(e.getMessage().contains("non-nullable"));
    assertTrue(e.getMessage().contains("id"));
  }

  @Test
  void missingNullableFieldIsSkipped() {
    // maybe is ["null","string"]; omitting it is allowed.
    Map<String, Object> avro =
        recordSchema(List.of(field("id", "int"), field("maybe", List.of("null", "string"))));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("id", 7), schema, descriptor);
    assertFalse(msg.hasField(descriptor.findFieldByName("maybe")));
  }

  @Test
  void explicitNullForNullableFieldIsSkipped() {
    Map<String, Object> avro =
        recordSchema(List.of(field("id", "int"), field("maybe", List.of("null", "string"))));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    Map<String, Object> record = new HashMap<>();
    record.put("id", 7);
    record.put("maybe", null);
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(record, schema, descriptor);
    assertFalse(msg.hasField(descriptor.findFieldByName("maybe")));
  }

  // --- #4: int32 exact conversion ---

  @Test
  void int32RejectsOutOfRangeLong() {
    Map<String, Object> avro = recordSchema(List.of(field("id", "int")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertThrows(
        ArithmeticException.class,
        () ->
            AvroToProtoDescriptorConverter.toDynamicMessage(
                Map.of("id", 3_000_000_000L), schema, descriptor));
  }

  @Test
  void int32RejectsFractional() {
    Map<String, Object> avro = recordSchema(List.of(field("id", "int")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroToProtoDescriptorConverter.toDynamicMessage(
                Map.of("id", 42.9), schema, descriptor));
  }

  @Test
  void int32AcceptsInRangeLong() {
    Map<String, Object> avro = recordSchema(List.of(field("id", "int")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("id", 123L), schema, descriptor);
    assertEquals(123, msg.getField(descriptor.findFieldByName("id")));
  }

  // --- #3 (round 2): descriptor label mirrors Avro nullability (SDK GenerateProto behavior) ---

  @Test
  void nonNullableScalarIsRequiredNullableIsOptional() {
    Map<String, Object> avro =
        recordSchema(List.of(field("id", "int"), field("maybe", List.of("null", "string"))));
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertTrue(descriptor.findFieldByName("id").isRequired());
    assertTrue(descriptor.findFieldByName("maybe").isOptional());
  }

  @Test
  void nestedRecordFieldIsRequiredWhenNonNullable() {
    Map<String, Object> nested =
        Map.of("type", "record", "name", "Addr", "fields", List.of(field("city", "string")));
    Map<String, Object> avro = recordSchema(List.of(field("addr", nested)));
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertTrue(descriptor.findFieldByName("addr").isRequired());
  }

  // --- #4 (round 2): null array elements / map values ---

  @Test
  void nullElementInNonNullableArrayThrows() {
    Map<String, Object> arrayType = Map.of("type", "array", "items", "string");
    Map<String, Object> avro = recordSchema(List.of(field("tags", arrayType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    List<String> tags = new java.util.ArrayList<>();
    tags.add("a");
    tags.add(null);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("tags", tags), schema, descriptor));
    assertTrue(e.getMessage().contains("Null array elements"));
    assertTrue(e.getMessage().contains("tags"));
  }

  @Test
  void nullElementInNullableArrayThrowsInsteadOfDropping() {
    // protobuf repeated fields have no null slots: dropping the null would change array length and
    // element positions. Even when the Avro element type is nullable, fail rather than corrupt.
    Map<String, Object> arrayType = Map.of("type", "array", "items", List.of("null", "string"));
    Map<String, Object> avro = recordSchema(List.of(field("tags", arrayType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    List<String> tags = new java.util.ArrayList<>();
    tags.add("a");
    tags.add(null);
    tags.add("b");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("tags", tags), schema, descriptor));
    assertTrue(e.getMessage().contains("Null array elements"));
  }

  @Test
  void nullValueInNonNullableMapThrows() {
    Map<String, Object> mapType = Map.of("type", "map", "values", "string");
    Map<String, Object> avro = recordSchema(List.of(field("labels", mapType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    Map<String, Object> labels = new HashMap<>();
    labels.put("env", "prod");
    labels.put("team", null);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("labels", labels), schema, descriptor));
    assertTrue(e.getMessage().contains("Null map values"));
    assertTrue(e.getMessage().contains("labels"));
  }

  @Test
  void nullValueInNullableMapThrowsInsteadOfDropping() {
    // Even a nullable map value type can't be represented: a protobuf map entry has no null value,
    // and skipping the entry silently drops its key.
    Map<String, Object> mapType = Map.of("type", "map", "values", List.of("null", "string"));
    Map<String, Object> avro = recordSchema(List.of(field("labels", mapType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    Map<String, Object> labels = new HashMap<>();
    labels.put("team", null);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("labels", labels), schema, descriptor));
    assertTrue(e.getMessage().contains("Null map values"));
  }

  @Test
  void nullKeyInMapThrows() {
    Map<String, Object> mapType = Map.of("type", "map", "values", "string");
    Map<String, Object> avro = recordSchema(List.of(field("labels", mapType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    Map<Object, Object> labels = new HashMap<>();
    labels.put(null, "prod");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("labels", labels), schema, descriptor));
    assertTrue(e.getMessage().contains("Null key"));
  }

  // --- #5 (round 2): exact int64 + strict boolean ---

  @Test
  void int64RejectsFractionalDouble() {
    Map<String, Object> avro = recordSchema(List.of(field("v", "long")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("v", 42.9), schema, descriptor));
  }

  @Test
  void int64AcceptsIntegralValue() {
    Map<String, Object> avro = recordSchema(List.of(field("v", "long")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("v", 9_000_000_000L), schema, descriptor);
    assertEquals(9_000_000_000L, msg.getField(descriptor.findFieldByName("v")));
  }

  @Test
  void boolRejectsGarbageString() {
    Map<String, Object> avro = recordSchema(List.of(field("b", "boolean")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("b", "not_a_bool"), schema, descriptor));
    assertTrue(e.getMessage().contains("Invalid boolean"));
  }

  @Test
  void boolAcceptsExactTrueFalseStrings() {
    Map<String, Object> avro = recordSchema(List.of(field("b", "boolean")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage t =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("b", "TRUE"), schema, descriptor);
    DynamicMessage f =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("b", "false"), schema, descriptor);
    assertEquals(true, t.getField(descriptor.findFieldByName("b")));
    assertEquals(false, f.getField(descriptor.findFieldByName("b")));
  }

  // --- round 3 #1: bytes/fixed strict decoding ---

  private static Descriptors.Descriptor bytesFieldDescriptor() {
    Map<String, Object> avro = recordSchema(List.of(field("payload", "bytes")));
    return AvroToProtoDescriptorConverter.toDescriptor(
        AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));
  }

  private static Schema bytesFieldSchema() {
    return AvroToProtoDescriptorConverter.parseAvro(
        recordSchema(List.of(field("payload", "bytes"))));
  }

  @Test
  void bytesFieldDecodesBase64String() {
    Descriptors.Descriptor descriptor = bytesFieldDescriptor();
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("payload", "AQID"), bytesFieldSchema(), descriptor);
    assertEquals(
        ByteString.copyFrom(new byte[] {1, 2, 3}),
        msg.getField(descriptor.findFieldByName("payload")));
  }

  @Test
  void bytesFieldRejectsNonBase64String() {
    Descriptors.Descriptor descriptor = bytesFieldDescriptor();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("payload", "not base64!!"), bytesFieldSchema(), descriptor));
    assertTrue(e.getMessage().contains("base64"));
  }

  @Test
  void bytesFieldAcceptsRawByteArray() {
    Descriptors.Descriptor descriptor = bytesFieldDescriptor();
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("payload", new byte[] {9, 8, 7}), bytesFieldSchema(), descriptor);
    assertEquals(
        ByteString.copyFrom(new byte[] {9, 8, 7}),
        msg.getField(descriptor.findFieldByName("payload")));
  }

  @Test
  void bytesFieldAcceptsByteBufferWithoutConsumingIt() {
    Descriptors.Descriptor descriptor = bytesFieldDescriptor();
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {4, 5, 6});
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(
            Map.of("payload", buffer), bytesFieldSchema(), descriptor);
    assertEquals(
        ByteString.copyFrom(new byte[] {4, 5, 6}),
        msg.getField(descriptor.findFieldByName("payload")));
    // the original buffer must not have been drained
    assertEquals(0, buffer.position());
  }

  @Test
  void fixedFieldAcceptsGenericFixed() {
    Map<String, Object> fixedType = Map.of("type", "fixed", "name", "Hash3", "size", 3);
    Map<String, Object> avro = recordSchema(List.of(field("h", fixedType)));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    Schema fixedSchema = schema.getField("h").schema();
    GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema, new byte[] {1, 2, 3});
    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("h", fixed), schema, descriptor);
    assertEquals(
        ByteString.copyFrom(new byte[] {1, 2, 3}), msg.getField(descriptor.findFieldByName("h")));
  }

  // --- full-pass #1: float/double finite + range ---

  @Test
  void floatRejectsOutOfRangeDouble() {
    Map<String, Object> avro = recordSchema(List.of(field("v", "float")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroToProtoDescriptorConverter.toDynamicMessage(
                Map.of("v", Double.MAX_VALUE), schema, descriptor));
  }

  @Test
  void floatAcceptsInRangeValue() {
    Map<String, Object> avro = recordSchema(List.of(field("v", "float")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("v", 1.5), schema, descriptor);
    assertEquals(1.5f, msg.getField(descriptor.findFieldByName("v")));
  }

  @Test
  void doubleRejectsNonFiniteValue() {
    Map<String, Object> avro = recordSchema(List.of(field("v", "double")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroToProtoDescriptorConverter.toDynamicMessage(
                Map.of("v", Double.NaN), schema, descriptor));
  }

  @Test
  void floatRejectsNaN() {
    Map<String, Object> avro = recordSchema(List.of(field("v", "float")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroToProtoDescriptorConverter.toDynamicMessage(
                Map.of("v", Float.NaN), schema, descriptor));
  }

  @Test
  void floatRejectsUnderflowToZero() {
    // Double.MIN_VALUE is finite and below Float.MIN_VALUE: floatValue() would silently return
    // 0.0f, writing zero for a non-zero source.
    Map<String, Object> avro = recordSchema(List.of(field("v", "float")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AvroToProtoDescriptorConverter.toDynamicMessage(
                    Map.of("v", Double.MIN_VALUE), schema, descriptor));
    assertTrue(e.getMessage().contains("Underflow"));
  }

  @Test
  void floatAcceptsExactZero() {
    // 0.0 must still be allowed — the underflow guard only rejects non-zero inputs that become 0.
    Map<String, Object> avro = recordSchema(List.of(field("v", "float")));
    Schema schema = AvroToProtoDescriptorConverter.parseAvro(avro);
    Descriptors.Descriptor descriptor =
        AvroToProtoDescriptorConverter.toDescriptor(
            AvroToProtoDescriptorConverter.toDescriptorProto(avro, "c.s.t"));

    DynamicMessage msg =
        AvroToProtoDescriptorConverter.toDynamicMessage(Map.of("v", 0.0), schema, descriptor);
    assertEquals(0.0f, msg.getField(descriptor.findFieldByName("v")));
  }
}
