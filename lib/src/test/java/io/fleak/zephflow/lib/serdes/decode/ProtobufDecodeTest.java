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
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
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

  private static final FileDescriptorProto PROTO3 =
      FileDescriptorProto.newBuilder()
          .setName("test.proto")
          .setPackage("test")
          .setSyntax("proto3")
          .addDependency("google/protobuf/timestamp.proto")
          .addDependency("google/protobuf/duration.proto")
          .addEnumType(
              EnumDescriptorProto.newBuilder()
                  .setName("Color")
                  .addValue(EnumValueDescriptorProto.newBuilder().setName("RED").setNumber(0))
                  .addValue(EnumValueDescriptorProto.newBuilder().setName("BLUE").setNumber(1)))
          .addMessageType(
              DescriptorProto.newBuilder()
                  .setName("Inner")
                  .addField(field("x", 1, Type.TYPE_STRING)))
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
                  .addOneofDecl(OneofDescriptorProto.newBuilder().setName("choice"))
                  .addOneofDecl(OneofDescriptorProto.newBuilder().setName("_opt"))
                  .addNestedType(
                      DescriptorProto.newBuilder()
                          .setName("MEntry")
                          .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                          .addField(field("key", 1, Type.TYPE_STRING))
                          .addField(field("value", 2, Type.TYPE_INT32))))
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
                  .addField(field("maybe", 2, Type.TYPE_INT32)))
          .build();

  private static final FileDescriptorSet SET =
      FileDescriptorSet.newBuilder()
          .addFile(PROTO3)
          .addFile(PROTO2)
          .addFile(Timestamp.getDescriptor().getFile().toProto())
          .addFile(Duration.getDescriptor().getFile().toProto())
          .build();

  private static final Descriptor ALL;
  private static final Descriptor REQ;

  static {
    try {
      FileDescriptor proto3 =
          FileDescriptor.buildFrom(
              PROTO3,
              new FileDescriptor[] {
                Timestamp.getDescriptor().getFile(), Duration.getDescriptor().getFile()
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

  private static final String SINGLE = config("test.All", "single");
  private static final String DELIMITED = config("test.All", "varint_delimited");

  private static final String DEFAULTS =
      "\"i32\":0,\"i64\":0,\"u32\":0,\"u64\":0,\"s32\":0,\"f32\":0,\"f64\":0,\"f\":0.0,\"d\":0.0,"
          + "\"b\":false,\"s\":\"\",\"by\":\"\",\"e\":\"RED\",\"rep\":[],\"m\":{}";

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
    byte[] input = b.build().toByteArray();
    assertEquals(
        List.of(
            new Ok(
                json(
                    "{\"i32\":-5,\"i64\":1099511627776,\"u32\":4294967295,\"u64\":18446744073709551615,"
                        + "\"s32\":-7,\"f32\":4294967294,\"f64\":3,\"f\":0.5,\"d\":\"NaN\",\"b\":true,"
                        + "\"s\":\"hi\",\"by\":\"AQID\",\"e\":\"BLUE\",\"inner\":{\"x\":\"in\"},"
                        + "\"rep\":[4,5],\"m\":{\"k\":9},\"ts\":-500,\"dur\":2005,\"opt\":0,\"c\":3}"),
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
    for (String config : bad) {
      assertThrows(IllegalArgumentException.class, () -> decoder(config), config);
    }
    assertEquals(
        List.of(new Ok(json("{\"x\":\"a\"}"), 1, new RawSpan(0, 3))),
        decode(config(".test.Inner", "single"), inner("a").toByteArray()));
  }
}
