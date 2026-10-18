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
package io.fleak.zephflow.lib.serdes.decode;

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ProtobufDecodeTest {

  private static FieldDescriptorProto field(String name, int number, Type type) {
    return FieldDescriptorProto.newBuilder()
        .setName(name)
        .setNumber(number)
        .setType(type)
        .setLabel(Label.LABEL_OPTIONAL)
        .build();
  }

  private static FieldDescriptorProto field(String name, int number, Type type, String typeName) {
    return field(name, number, type).toBuilder().setTypeName(typeName).build();
  }

  private static DescriptorProto mapEntry(String name, Type keyType) {
    return DescriptorProto.newBuilder()
        .setName(name)
        .setOptions(MessageOptions.newBuilder().setMapEntry(true))
        .addField(field("key", 1, keyType))
        .addField(field("value", 2, Type.TYPE_INT32))
        .build();
  }

  private static FieldDescriptorProto mapField(String name, int number, String entry) {
    return field(name, number, Type.TYPE_MESSAGE, ".test.All." + entry).toBuilder()
        .setLabel(Label.LABEL_REPEATED)
        .build();
  }

  /** A message of 100 int32 fields, so an empty one becomes 100 defaults. */
  private static DescriptorProto wide() {
    DescriptorProto.Builder wide = DescriptorProto.newBuilder().setName("Wide");
    for (int i = 1; i <= 100; i++) {
      wide.addField(field("f" + i, i, Type.TYPE_INT32));
    }
    return wide.build();
  }

  /** A proto2 message of 200 optional fields, which appear only when set. */
  private static DescriptorProto sparse() {
    DescriptorProto.Builder sparse = DescriptorProto.newBuilder().setName("Sparse");
    for (int i = 1; i <= 200; i++) {
      sparse.addField(field("f" + i, i, Type.TYPE_INT32));
    }
    return sparse.build();
  }

  private static final FileDescriptorProto PROTO3 =
      FileDescriptorProto.newBuilder()
          .setName("test.proto")
          .setPackage("test")
          .setSyntax("proto3")
          .addDependency("google/protobuf/timestamp.proto")
          .addDependency("google/protobuf/duration.proto")
          .addDependency("google/protobuf/wrappers.proto")
          .addEnumType(
              EnumDescriptorProto.newBuilder()
                  .setName("Color")
                  .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0))
                  .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(1)))
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Inner")
                  .addField(field("x", 1, Type.TYPE_STRING)))
          .addMessageType(wide())
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Holder")
                  .addField(
                      field("w", 1, Type.TYPE_MESSAGE, ".test.Wide").toBuilder()
                          .setLabel(Label.LABEL_REPEATED)))
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Index")
                  .addField(
                      field("byKey", 1, Type.TYPE_MESSAGE, ".test.Index.ByKeyEntry").toBuilder()
                          .setLabel(Label.LABEL_REPEATED))
                  .addNestedType(
                      DescriptorProto.newBuilder()
                          .setName("ByKeyEntry")
                          .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                          .addField(field("key", 1, Type.TYPE_INT32))
                          .addField(field("value", 2, Type.TYPE_MESSAGE, ".test.Wide"))))
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("All")
                  .addField(field("i32", 1, Type.TYPE_INT32))
                  .addField(field("i64", 2, Type.TYPE_INT64))
                  .addField(field("u32", 3, Type.TYPE_UINT32))
                  .addField(field("u64", 4, Type.TYPE_UINT64))
                  .addField(field("s32", 5, Type.TYPE_SINT32))
                  .addField(field("f32", 6, Type.TYPE_FIXED32))
                  .addField(field("f64", 7, Type.TYPE_FIXED64))
                  .addField(field("f", 8, Type.TYPE_FLOAT))
                  .addField(field("d", 9, Type.TYPE_DOUBLE))
                  .addField(field("b", 10, Type.TYPE_BOOL))
                  .addField(field("s", 11, Type.TYPE_STRING))
                  .addField(field("by", 12, Type.TYPE_BYTES))
                  .addField(field("e", 13, Type.TYPE_ENUM, ".test.Color"))
                  .addField(field("inner", 14, Type.TYPE_MESSAGE, ".test.Inner"))
                  .addField(
                      field("rep", 15, Type.TYPE_INT32).toBuilder().setLabel(Label.LABEL_REPEATED))
                  .addField(
                      field("m", 16, Type.TYPE_MESSAGE, ".test.All.MEntry").toBuilder()
                          .setLabel(Label.LABEL_REPEATED))
                  .addField(field("ts", 17, Type.TYPE_MESSAGE, ".google.protobuf.Timestamp"))
                  .addField(field("dur", 18, Type.TYPE_MESSAGE, ".google.protobuf.Duration"))
                  .addField(
                      field("opt", 19, Type.TYPE_INT32).toBuilder()
                          .setProto3Optional(true)
                          .setOneofIndex(1))
                  .addField(field("a", 20, Type.TYPE_STRING).toBuilder().setOneofIndex(0))
                  .addField(field("c", 21, Type.TYPE_INT32).toBuilder().setOneofIndex(0))
                  .addField(field("s64", 22, Type.TYPE_SINT64))
                  .addField(field("sf32", 23, Type.TYPE_SFIXED32))
                  .addField(field("sf64", 24, Type.TYPE_SFIXED64))
                  .addField(mapField("mi", 25, "MiEntry"))
                  .addField(mapField("mb", 26, "MbEntry"))
                  .addField(mapField("mu", 27, "MuEntry"))
                  .addField(
                      field("inners", 28, Type.TYPE_MESSAGE, ".test.Inner").toBuilder()
                          .setLabel(Label.LABEL_REPEATED))
                  .addField(field("wrapped", 29, Type.TYPE_MESSAGE, ".google.protobuf.Int64Value"))
                  .addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice"))
                  .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_opt"))
                  .addNestedType(mapEntry("MEntry", Type.TYPE_STRING))
                  .addNestedType(mapEntry("MiEntry", Type.TYPE_INT32))
                  .addNestedType(mapEntry("MbEntry", Type.TYPE_BOOL))
                  .addNestedType(mapEntry("MuEntry", Type.TYPE_UINT64)))
          .build();

  private static final FileDescriptorProto PROTO2 =
      FileDescriptorProto.newBuilder()
          .setName("old.proto")
          .setPackage("old")
          .setSyntax("proto2")
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Req")
                  .addField(
                      field("need", 1, Type.TYPE_INT32).toBuilder().setLabel(Label.LABEL_REQUIRED))
                  .addField(field("maybe", 2, Type.TYPE_INT32))
                  .addField(field("color", 3, Type.TYPE_ENUM, ".old.Shade")))
          .addMessageType(sparse())
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Bag")
                  .addField(
                      field("items", 1, Type.TYPE_MESSAGE, ".old.Sparse").toBuilder()
                          .setLabel(Label.LABEL_REPEATED))
                  .addField(field("grp", 5, Type.TYPE_GROUP, ".old.Bag.Grp"))
                  .addNestedType(
                      DescriptorProto.newBuilder()
                          .setName("Grp")
                          .addField(field("v", 1, Type.TYPE_INT32))))
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Node")
                  .addField(field("next", 1, Type.TYPE_MESSAGE, ".old.Node")))
          .addEnumType(
              EnumDescriptorProto.newBuilder()
                  .setName("Shade")
                  .addValue(EnumValueDescriptorProto.newBuilder().setName("DARK").setNumber(0)))
          .build();

  private static final FileDescriptorSet SET =
      FileDescriptorSet.newBuilder()
          .addFile(PROTO3)
          .addFile(PROTO2)
          .addFile(Timestamp.getDescriptor().getFile().toProto())
          .addFile(Duration.getDescriptor().getFile().toProto())
          .addFile(Int64Value.getDescriptor().getFile().toProto())
          .build();

  private static final Descriptor ALL;
  private static final Descriptor REQ;

  static {
    try {
      FileDescriptor proto3 =
          FileDescriptor.buildFrom(
              PROTO3,
              new FileDescriptor[] {
                Timestamp.getDescriptor().getFile(),
                Duration.getDescriptor().getFile(),
                Int64Value.getDescriptor().getFile()
              });
      ALL = proto3.findMessageTypeByName("All");
      REQ = FileDescriptor.buildFrom(PROTO2, new FileDescriptor[0]).findMessageTypeByName("Req");
    } catch (DescriptorValidationException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String config(String messageType, String framing) {
    return "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
        + Base64.getEncoder().encodeToString(SET.toByteArray())
        + "\",\"messageType\":\""
        + messageType
        + "\",\"framing\":\""
        + framing
        + "\"}}";
  }

  private static String setConfig(FileDescriptorSet set, String messageType) {
    return "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
        + Base64.getEncoder().encodeToString(set.toByteArray())
        + "\",\"messageType\":\""
        + messageType
        + "\",\"framing\":\"single\"}}";
  }

  private static final String SINGLE = config("test.All", "single");
  private static final String DELIMITED = config("test.All", "varint_delimited");

  private static final String DEFAULTS =
      "\"i32\":0,\"i64\":0,\"u32\":0,\"u64\":0,\"s32\":0,\"f32\":0,\"f64\":0,\"f\":0.0,\"d\":0.0,"
          + "\"b\":false,\"s\":\"\",\"by\":\"\",\"e\":\"RED\",\"rep\":[],\"m\":{},\"s64\":0,"
          + "\"sf32\":0,\"sf64\":0,\"mi\":{},\"mb\":{},\"mu\":{},\"inners\":[]";

  private static DynamicMessage.Builder all() {
    return DynamicMessage.newBuilder(ALL);
  }

  private static DynamicMessage.Builder set(
      DynamicMessage.Builder builder, String name, Object value) {
    return builder.setField(ALL.findFieldByName(name), value);
  }

  private static byte[] delimited(Message... messages) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      for (Message message : messages) {
        message.writeDelimitedTo(bytes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  private static void putEntry(
      DynamicMessage.Builder builder, String field, Object key, int value) {
    Descriptor entry = ALL.findFieldByName(field).getMessageType();
    builder.addRepeatedField(
        ALL.findFieldByName(field),
        DynamicMessage.newBuilder(entry)
            .setField(entry.findFieldByName("key"), key)
            .setField(entry.findFieldByName("value"), value)
            .build());
  }

  @Test
  void everyTypeMaps() {
    DynamicMessage.Builder b = all();
    set(b, "i32", -5);
    set(b, "i64", 1L << 40);
    set(b, "u32", -1);
    set(b, "u64", -1L);
    set(b, "s32", -7);
    set(b, "f32", -2);
    set(b, "f64", 3L);
    set(b, "f", 0.5f);
    set(b, "d", Double.NaN);
    set(b, "b", true);
    set(b, "s", "hi");
    set(b, "by", ByteString.copyFrom(new byte[] {1, 2, 3}));
    set(b, "e", ALL.findFieldByName("e").getEnumType().findValueByName("BLUE"));
    set(
        b,
        "inner",
        DynamicMessage.newBuilder(ALL.findFieldByName("inner").getMessageType())
            .setField(ALL.findFieldByName("inner").getMessageType().findFieldByName("x"), "in")
            .build());
    b.addRepeatedField(ALL.findFieldByName("rep"), 4);
    b.addRepeatedField(ALL.findFieldByName("rep"), 5);
    Descriptor entry = ALL.findFieldByName("m").getMessageType();
    b.addRepeatedField(
        ALL.findFieldByName("m"),
        DynamicMessage.newBuilder(entry)
            .setField(entry.findFieldByName("key"), "k")
            .setField(entry.findFieldByName("value"), 9)
            .build());
    set(b, "ts", Timestamp.newBuilder().setSeconds(-1).setNanos(500_000_000).build());
    set(b, "dur", Duration.newBuilder().setSeconds(2).setNanos(5_000_000).build());
    set(b, "opt", 0);
    set(b, "c", 3);
    set(b, "s64", -9L);
    set(b, "sf32", -3);
    set(b, "sf64", Long.MIN_VALUE);
    putEntry(b, "mi", -4, 1);
    putEntry(b, "mb", true, 2);
    putEntry(b, "mu", -1L, 3);
    b.addRepeatedField(ALL.findFieldByName("inners"), inner("p"));
    b.addRepeatedField(ALL.findFieldByName("inners"), inner("q"));
    set(b, "wrapped", Int64Value.of(5));
    byte[] input = b.build().toByteArray();
    assertEquals(
        List.of(
            new Ok(
                json(
                    "{\"i32\":-5,\"i64\":1099511627776,\"u32\":4294967295,\"u64\":18446744073709551615,"
                        + "\"s32\":-7,\"f32\":4294967294,\"f64\":3,\"f\":0.5,\"d\":\"NaN\",\"b\":true,"
                        + "\"s\":\"hi\",\"by\":\"AQID\",\"e\":\"BLUE\",\"inner\":{\"x\":\"in\"},"
                        + "\"rep\":[4,5],\"m\":{\"k\":9},\"ts\":-500,\"dur\":2005,\"opt\":0,\"c\":3,"
                        + "\"s64\":-9,\"sf32\":-3,\"sf64\":-9223372036854775808,\"mi\":{\"-4\":1},"
                        + "\"mb\":{\"true\":2},\"mu\":{\"18446744073709551615\":3},"
                        + "\"inners\":[{\"x\":\"p\"},{\"x\":\"q\"}],\"wrapped\":{\"value\":5}}"),
                1,
                new RawSpan(0, input.length))),
        decode(SINGLE, input));
  }

  @Test
  void emptyInputIsAMessageWithEveryProto3ScalarAtItsDefault() {
    assertEquals(
        List.of(new Ok(json("{" + DEFAULTS + "}"), 1, new RawSpan(0, 0))),
        decode(SINGLE, new byte[0]));
  }

  @Test
  void unknownEnumNumbersComeOutAsNumbers() {
    byte[] input = {(byte) (13 << 3), 7};
    assertEquals(
        List.of(
            new Ok(
                json("{" + DEFAULTS.replace("\"e\":\"RED\"", "\"e\":7") + "}"),
                1,
                new RawSpan(0, 2))),
        decode(SINGLE, input));
  }

  @Test
  void proto2PresenceAndRequiredFields() {
    String config = config("old.Req", "single");
    byte[] withRequired = {8, 1};
    assertEquals(
        List.of(new Ok(json("{\"need\":1}"), 1, new RawSpan(0, 2))), decode(config, withRequired));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                -1,
                new RawSpan(0, 0),
                "UninitializedMessageException: Message missing required fields: need",
                true)),
        decode(config, new byte[0]));
  }

  @Test
  void singleMessageThatDoesNotParseIsTerminal() {
    byte[] input = {(byte) 0x0A, 5, 'a'};
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(input, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, 3),
                "InvalidProtocolBufferException: While parsing a protocol message, the input ended unexpectedly in the middle of a field.  This could mean either that the input has been truncated or that an embedded message misreported its own length.",
                true)),
        decode(config("test.Inner", "single"), input));
  }

  private static Message inner(String x) {
    Descriptor type = ALL.findFieldByName("inner").getMessageType();
    return DynamicMessage.newBuilder(type).setField(type.findFieldByName("x"), x).build();
  }

  private static final String INNER = config("test.Inner", "varint_delimited");

  @Test
  void delimitedMessagesHaveExactSpansAndABadMessageLosesOnlyItself() {
    byte[] bad = {3, (byte) 0x0A, 5, 'a'};
    byte[] input = concat(delimited(inner("a")), bad, delimited(inner("bc")));
    assertEquals(
        List.of(
            new Ok(json("{\"x\":\"a\"}"), 1, new RawSpan(1, 3)),
            new Bad(
                ErrorKind.MALFORMED,
                new String(Arrays.copyOfRange(bad, 1, 4), StandardCharsets.UTF_8),
                2,
                new RawSpan(5, 3),
                "InvalidProtocolBufferException: While parsing a protocol message, the input ended unexpectedly in the middle of a field.  This could mean either that the input has been truncated or that an embedded message misreported its own length.",
                false),
            new Ok(json("{\"x\":\"bc\"}"), 3, new RawSpan(9, 4))),
        decode(INNER, input));
  }

  @Test
  void delimitedMessageOverTheLimitIsSkipped() {
    byte[] input = concat(delimited(inner("x".repeat(100))), delimited(inner("ok")));
    assertEquals(
        List.of(
            new Big(1, new RawSpan(1, 102), 102, "\n" + (char) 100 + "x".repeat(100), false),
            new Ok(json("{\"x\":\"ok\"}"), 2, new RawSpan(104, 4))),
        decode(decoder(INNER), ByteInput.ofBytes(input), new DecodeLimits(50, 0)));
  }

  @Test
  void brokenPrefixesAndCutOffMessagesAreTerminal() {
    byte[] whole = concat(delimited(inner("a")), delimited(inner("bcdef")));
    assertEquals(
        List.of(
            new Ok(json("{\"x\":\"a\"}"), 1, new RawSpan(1, 3)),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                RawSpan.toEnd(4),
                "MalformedProtobufException: input ends inside message 2",
                true)),
        decode(INNER, Arrays.copyOf(whole, whole.length - 2)));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                RawSpan.toEnd(0),
                "MalformedProtobufException: length of message 1 is longer than 5 bytes",
                true)),
        decode(INNER, new byte[] {-1, -1, -1, -1, -1, 1}));
    assertEquals(List.of(), decode(INNER, new byte[0]));
  }

  @Test
  void readFailureIsNotMalformed() {
    byte[] input = concat(delimited(inner("a")), delimited(inner("b")));
    assertEquals(
        List.of(
            new Ok(json("{\"x\":\"a\"}"), 1, new RawSpan(1, 3)), new Threw("SourceReadException")),
        decode(decoder(INNER), failingAt(input, 5), DecodeLimits.UNLIMITED));
  }

  @Test
  void singleMessageOverTheDocumentLimitIsOversized() {
    byte[] input = inner("x".repeat(20)).toByteArray();
    assertEquals(
        List.of(
            new Big(
                -1,
                RawSpan.toEnd(0),
                input.length,
                new String(input, StandardCharsets.UTF_8),
                true)),
        decode(
            decoder(config("test.Inner", "single")),
            ByteInput.ofBytes(input),
            new DecodeLimits(0, 10)));
  }

  @Test
  void configErrorsAreReportedAtCompileTime() {
    String missingDependency =
        Base64.getEncoder()
            .encodeToString(FileDescriptorSet.newBuilder().addFile(PROTO3).build().toByteArray());
    String set = Base64.getEncoder().encodeToString(SET.toByteArray());
    List<String> bad =
        List.of(
            "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
                + set
                + "\",\"messageType\":\"test.All\"}}",
            "{\"format\":{\"type\":\"protobuf\",\"messageType\":\"test.All\",\"framing\":\"single\"}}",
            "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\"!!!\",\"messageType\":\"test.All\",\"framing\":\"single\"}}",
            "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
                + set
                + "\",\"messageType\":\"test.Nope\",\"framing\":\"single\"}}",
            "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
                + missingDependency
                + "\",\"messageType\":\"test.All\",\"framing\":\"single\"}}");
    FileDescriptorProto otherA = PROTO2.toBuilder().setPackage("other").build();
    FileDescriptorProto cycleA =
        FileDescriptorProto.newBuilder().setName("a.proto").addDependency("b.proto").build();
    FileDescriptorProto cycleB =
        FileDescriptorProto.newBuilder().setName("b.proto").addDependency("a.proto").build();
    FileDescriptorProto invalid =
        FileDescriptorProto.newBuilder()
            .setName("bad.proto")
            .setPackage("bad")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("B")
                    .addField(field("x", 1, Type.TYPE_MESSAGE, ".nope.X")))
            .build();
    List<String> moreBad = new ArrayList<>(bad);
    moreBad.add(
        setConfig(
            FileDescriptorSet.newBuilder().addFile(PROTO2).addFile(otherA).build(), "old.Req"));
    moreBad.add(
        setConfig(FileDescriptorSet.newBuilder().addFile(cycleA).addFile(cycleB).build(), "x.Y"));
    moreBad.add(setConfig(FileDescriptorSet.newBuilder().addFile(invalid).build(), "bad.B"));
    moreBad.add(setConfig(SET, "test.All.MEntry"));
    assertEquals(
        "protobuf descriptorSet is not a base64 FileDescriptorSet: Illegal base64 character 21",
        assertThrows(IllegalArgumentException.class, () -> decoder(bad.get(2))).getMessage());
    moreBad.add(
        "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\"AAAA\",\"messageType\":\"test.All\",\"framing\":\"single\"}}");
    moreBad.add(
        "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
            + set
            + "\",\"framing\":\"single\"}}");
    for (String config : moreBad) {
      assertThrows(IllegalArgumentException.class, () -> decoder(config), config);
    }
    assertThrows(
        UncheckedIOException.class,
        () ->
            decoder(
                "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
                    + set
                    + "\",\"messageType\":\"test.All\",\"framing\":\"nope\"}}"));
    String wrapped =
        Base64.getMimeEncoder().encodeToString(SET.toByteArray()).replace("\r\n", "\\n");
    assertEquals(
        List.of(new Ok(json("{\"x\":\"a\"}"), 1, new RawSpan(0, 3))),
        decode(
            "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
                + wrapped
                + "\",\"messageType\":\"test.Inner\",\"framing\":\"single\"}}",
            inner("a").toByteArray()));
    assertEquals(
        List.of(new Ok(json("{\"x\":\"a\"}"), 1, new RawSpan(0, 3))),
        decode(config(".test.Inner", "single"), inner("a").toByteArray()));
  }

  @Test
  void unsignedValuesAbove2To63AreLargeNumbersNotSaturated() {
    for (Map.Entry<Long, Double> unsigned :
        Map.of(-1L, 1.8446744073709552E19, -4530646269038590975L, 1.3916097804670962E19)
            .entrySet()) {
      byte[] input = set(all(), "u64", unsigned.getKey()).build().toByteArray();
      Map<?, ?> record = (Map<?, ?>) ((Ok) decode(SINGLE, input).get(0)).record().unwrap();
      assertEquals(unsigned.getValue(), record.get("u64"));
    }
  }

  @Test
  void timesRoundDownAndOverflowIsMalformed() {
    byte[] negative =
        set(all(), "dur", Duration.newBuilder().setSeconds(-1).setNanos(-500_000).build())
            .build()
            .toByteArray();
    assertEquals(
        List.of(
            new Ok(json("{" + DEFAULTS + ",\"dur\":-1001}"), 1, new RawSpan(0, negative.length))),
        decode(SINGLE, negative));
    byte[] overflow =
        set(all(), "ts", Timestamp.newBuilder().setSeconds(Long.MAX_VALUE).build())
            .build()
            .toByteArray();
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(overflow, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, overflow.length),
                "MalformedProtobufException: google.protobuf.Timestamp of 9223372036854775807 seconds is out of range",
                true)),
        decode(SINGLE, overflow));
  }

  @Test
  void unknownFieldsAreIgnoredAndClosedEnumsDropUnknownValues() {
    assertEquals(
        List.of(new Ok(json("{" + DEFAULTS + "}"), 1, new RawSpan(0, 3))),
        decode(SINGLE, new byte[] {(byte) 0x98, 0x06, 1}));
    assertEquals(
        List.of(new Ok(json("{\"need\":1}"), 1, new RawSpan(0, 4))),
        decode(config("old.Req", "single"), new byte[] {8, 1, 24, 7}));
  }

  @Test
  void emptySubmessagesCannotExpandWithoutBound() {
    String holder = config("test.Holder", "single");
    byte[] few = new byte[20];
    for (int i = 0; i < few.length; i += 2) {
      few[i] = 0x0A;
    }
    StringBuilder zeros = new StringBuilder("{");
    for (int i = 1; i <= 100; i++) {
      zeros.append(i == 1 ? "" : ",").append("\"f").append(i).append("\":0");
    }
    String wide = zeros.append('}').toString();
    assertEquals(
        List.of(
            new Ok(
                json("{\"w\":[" + String.join(",", Collections.nCopies(10, wide)) + "]}"),
                1,
                new RawSpan(0, 20))),
        decode(holder, few));
    byte[] many = new byte[40_000];
    for (int i = 0; i < many.length; i += 2) {
      many[i] = 0x0A;
    }
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(many, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, many.length),
                "MalformedProtobufException: message would build more than 4 values per byte plus 100000",
                true)),
        decode(holder, many));
  }

  @Test
  void mapEntriesWithoutAValueAreChargedForItsDefaults() {
    String index = config("test.Index", "single");
    StringBuilder zeros = new StringBuilder("{");
    for (int i = 1; i <= 100; i++) {
      zeros.append(i == 1 ? "" : ",").append("\"f").append(i).append("\":0");
    }
    String wide = zeros.append('}').toString();
    assertEquals(
        List.of(
            new Ok(
                json("{\"byKey\":{\"1\":" + wide + ",\"2\":" + wide + "}}"), 1, new RawSpan(0, 8))),
        decode(index, new byte[] {0x0A, 2, 8, 1, 0x0A, 2, 8, 2}));
    byte[] many = new byte[8000];
    for (int i = 0; i < many.length; i += 4) {
      many[i] = 0x0A;
      many[i + 1] = 2;
      many[i + 2] = 8;
      many[i + 3] = 1;
    }
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                new String(many, StandardCharsets.UTF_8),
                -1,
                new RawSpan(0, many.length),
                "MalformedProtobufException: message would build more than 4 values per byte plus 100000",
                true)),
        decode(index, many));
  }

  @Test
  void sparseSubmessagesOfAWideTypeAreNotChargedForUnsetFields() {
    byte[] items = new byte[4000];
    for (int i = 0; i < items.length; i += 4) {
      items[i] = 0x0A;
      items[i + 1] = 2;
      items[i + 2] = 8;
      items[i + 3] = 1;
    }
    assertEquals(
        List.of(
            new Ok(
                json(
                    "{\"items\":["
                        + String.join(",", Collections.nCopies(1000, "{\"f1\":1}"))
                        + "]}"),
                1,
                new RawSpan(0, items.length))),
        decode(config("old.Bag", "single"), items));
  }

  @Test
  void aGroupFieldIsOnlyReadAsAGroup() {
    String bag = config("old.Bag", "single");
    assertEquals(
        List.of(new Ok(json("{\"items\":[],\"grp\":{\"v\":7}}"), 1, new RawSpan(0, 4))),
        decode(bag, new byte[] {0x2B, 8, 7, 0x2C}));
    assertEquals(
        List.of(new Ok(json("{\"items\":[]}"), 1, new RawSpan(0, 4))),
        decode(bag, new byte[] {0x2A, 2, (byte) 0xFF, (byte) 0xFF}));
  }

  /** {@code levels} Nodes nested in a top-level Node, each the only field of its parent. */
  private static byte[] nested(int levels) {
    byte[] node = new byte[0];
    for (int i = 0; i < levels; i++) {
      ByteArrayOutputStream outer = new ByteArrayOutputStream();
      outer.write(0x0A);
      outer.write(node.length < 128 ? node.length : node.length & 0x7F | 0x80);
      if (node.length >= 128) {
        outer.write(node.length >>> 7);
      }
      outer.writeBytes(node);
      node = outer.toByteArray();
    }
    return node;
  }

  /** {@code levels} unknown groups (field 9) nested in a Node. */
  private static byte[] unknownGroups(int levels) {
    byte[] groups = new byte[2 * levels];
    Arrays.fill(groups, 0, levels, (byte) 0x4B);
    Arrays.fill(groups, levels, 2 * levels, (byte) 0x4C);
    return groups;
  }

  private static byte[] framed(byte[] message) {
    ByteArrayOutputStream framed = new ByteArrayOutputStream();
    framed.write(message.length < 128 ? message.length : message.length & 0x7F | 0x80);
    if (message.length >= 128) {
      framed.write(message.length >>> 7);
    }
    framed.writeBytes(message);
    return framed.toByteArray();
  }

  @Test
  void messagesNestedTooDeeplyLoseOnlyThemselves() {
    String node = config("old.Node", "varint_delimited");
    byte[] deepest = nested(100);
    byte[] tooDeep = nested(101);
    byte[] groups = unknownGroups(100);
    byte[] tooManyGroups = unknownGroups(101);
    byte[] input =
        concat(
            framed(deepest),
            framed(tooDeep),
            framed(groups),
            framed(tooManyGroups),
            framed(new byte[0]));
    String chain = "{\"next\":".repeat(100) + "{}" + "}".repeat(100);
    int at = 2 + deepest.length;
    int groupsAt = at + 2 + tooDeep.length;
    int tooManyAt = groupsAt + 2 + groups.length;
    assertEquals(
        List.of(
            new Ok(json(chain), 1, new RawSpan(2, deepest.length)),
            new Bad(
                ErrorKind.MALFORMED,
                new String(tooDeep, StandardCharsets.UTF_8),
                2,
                new RawSpan(at + 2, tooDeep.length),
                "MalformedProtobufException: message nests deeper than 100 levels",
                false),
            new Ok(json("{}"), 3, new RawSpan(groupsAt + 2, groups.length)),
            new Bad(
                ErrorKind.MALFORMED,
                new String(tooManyGroups, StandardCharsets.UTF_8),
                4,
                new RawSpan(tooManyAt + 2, tooManyGroups.length),
                "MalformedProtobufException: message nests deeper than 100 levels",
                false),
            new Ok(json("{}"), 5, new RawSpan(tooManyAt + 2 + tooManyGroups.length + 1, 0))),
        decode(node, input));
  }

  @Test
  void limitsAreExactForBothFramings() {
    byte[] message = inner("abc").toByteArray();
    byte[] framed = delimited(inner("abc"));
    String single = config("test.Inner", "single");
    assertEquals(
        List.of(new Ok(json("{\"x\":\"abc\"}"), 1, new RawSpan(0, message.length))),
        decode(decoder(single), ByteInput.ofBytes(message), new DecodeLimits(0, message.length)));
    assertEquals(
        List.of(
            new Big(
                -1,
                RawSpan.toEnd(0),
                message.length,
                new String(message, StandardCharsets.UTF_8),
                true)),
        decode(
            decoder(single), ByteInput.ofBytes(message), new DecodeLimits(0, message.length - 1)));
    assertEquals(
        List.of(new Ok(json("{\"x\":\"abc\"}"), 1, new RawSpan(1, message.length))),
        decode(decoder(INNER), ByteInput.ofBytes(framed), new DecodeLimits(message.length, 0)));
    assertEquals(
        List.of(
            new Big(
                1,
                new RawSpan(1, message.length),
                message.length,
                new String(message, StandardCharsets.UTF_8),
                false)),
        decode(decoder(INNER), ByteInput.ofBytes(framed), new DecodeLimits(message.length - 1, 0)));
  }
}
