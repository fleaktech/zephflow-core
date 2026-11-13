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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos.*;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.serdes.decode.config.*;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class DecodingConfigurationDiagnosticsTest {
  static Stream<Arguments> avroAndUnsupportedFormatErrors() {
    return Stream.of(
        Arguments.of("{\"type\":\"kv\"}", of("format", "type"), "cannot be read"),
        Arguments.of("{\"type\":\"avro\"}", of("format", "container"), "ocf, datum"),
        Arguments.of(
            "{\"type\":\"avro\",\"container\":\"ocf\",\"schema\":{}}",
            of("format", "schema"),
            "absent"),
        Arguments.of(
            "{\"type\":\"avro\",\"container\":\"datum\"}", of("format", "schema"), "required"),
        Arguments.of(
            "{\"type\":\"avro\",\"container\":\"datum\",\"schema\":{\"type\":\"do-not-copy-this\"}}",
            of("format", "schema"),
            "Invalid Avro"),
        Arguments.of(
            "{\"type\":\"avro\",\"container\":\"datum\",\"schema\":{\"type\":\"string\"}}",
            of("format", "schema"),
            "type record"));
  }

  static Stream<Arguments> csvFormatErrors() {
    return Stream.of(
        Arguments.of(
            "{\"type\":\"csv\",\"delimiter\":\";;\"}", of("format", "delimiter"), "single ASCII"),
        Arguments.of("{\"type\":\"csv\",\"quote\":\"ab\"}", of("format", "quote"), "empty string"),
        Arguments.of(
            "{\"type\":\"csv\",\"escape\":\"ab\"}", of("format", "escape"), "single ASCII"),
        Arguments.of(
            "{\"type\":\"csv\",\"commentMarker\":\"ab\"}",
            of("format", "commentMarker"),
            "single ASCII"),
        Arguments.of("{\"type\":\"csv\",\"skipLines\":-1}", of("format", "skipLines"), "0"),
        Arguments.of("{\"type\":\"csv\",\"columns\":[]}", of("format", "columns"), "at least one"),
        Arguments.of(
            "{\"type\":\"csv\",\"columns\":[\"a\",null]}",
            of("format", "columns").index(1),
            "non-null"),
        Arguments.of(
            "{\"type\":\"csv\",\"columns\":[\"a\",\"\"]}",
            of("format", "columns").index(1),
            "non-empty"),
        Arguments.of(
            "{\"type\":\"csv\",\"columns\":[\"do-not-copy-this\",\"do-not-copy-this\"]}",
            of("format", "columns").index(1),
            "unique"),
        Arguments.of(
            "{\"type\":\"csv\",\"charset\":\"do-not-copy-this\"}",
            of("format", "charset"),
            "supported charset"));
  }

  static Stream<Arguments> structuredFormatErrors() {
    return Stream.of(
        Arguments.of("{\"type\":\"json\"}", of("format", "mode"), "object, array, lines"),
        Arguments.of("{\"type\":\"yaml\"}", of("format", "mode"), "document, multi_document"),
        Arguments.of("{\"type\":\"otlp\"}", of("format", "signal"), "logs, metrics, traces"),
        Arguments.of("{\"type\":\"xml\"}", of("format", "mode"), "document, elements"),
        Arguments.of(
            "{\"type\":\"xml\",\"mode\":\"document\",\"recordPath\":\"/a\"}",
            of("format", "recordPath"),
            "absent"),
        Arguments.of(
            "{\"type\":\"xml\",\"mode\":\"elements\"}",
            of("format", "recordPath"),
            "absolute path"),
        Arguments.of(
            "{\"type\":\"xml\",\"mode\":\"elements\",\"recordPath\":\"/a/*\"}",
            of("format", "recordPath"),
            "wildcards"));
  }

  static Stream<Arguments> textAndProtobufFormatErrors() {
    return Stream.of(
        Arguments.of("{\"type\":\"text\"}", of("format", "framing"), "line, multiline, whole"),
        Arguments.of(
            "{\"type\":\"text\",\"charset\":\"do-not-copy-this\"}",
            of("format", "charset"),
            "supported charset"),
        Arguments.of(
            "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"a\",\"maxLines\":0}}",
            of("format", "framing", "maxLines"),
            "1"),
        Arguments.of(
            "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"do-not-copy-this(\"}}",
            of("format", "framing", "startPattern"),
            "index"),
        Arguments.of(
            "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"continuePattern\":\"do-not-copy-this(\"}}",
            of("format", "framing", "continuePattern"),
            "index"),
        Arguments.of(
            "{\"type\":\"protobuf\"}", of("format", "framing"), "single, varint_delimited"),
        Arguments.of(
            "{\"type\":\"protobuf\",\"framing\":\"single\",\"descriptorSet\":\"do-not-copy-this\",\"messageType\":\"Event\"}",
            of("format", "descriptorSet"),
            "base64"));
  }

  @ParameterizedTest
  @MethodSource({
    "avroAndUnsupportedFormatErrors",
    "csvFormatErrors",
    "structuredFormatErrors",
    "textAndProtobufFormatErrors"
  })
  void identifiesExistingFormatRejections(String format, ConfigurationPath path, String constraint)
      throws Exception {
    var config = OBJECT_MAPPER.readValue("{\"format\":" + format + "}", DecodingConfig.class);
    assertIssue(() -> DecodingConfigCompiler.compile(config), path, constraint);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"type\":\"auto\"}",
        "{\"type\":\"avro\",\"container\":\"ocf\"}",
        "{\"type\":\"avro\",\"container\":\"datum\",\"schema\":{\"type\":\"record\",\"name\":\"Event\",\"fields\":[]}}",
        "{\"type\":\"csv\",\"delimiter\":\";\",\"quote\":\"\",\"escape\":\"\",\"commentMarker\":\"\",\"skipLines\":0,\"columns\":[\" \"],\"charset\":\"UTF-8\"}",
        "{\"type\":\"json\",\"mode\":\"object\"}",
        "{\"type\":\"yaml\",\"mode\":\"multi_document\"}",
        "{\"type\":\"xml\",\"mode\":\"document\"}",
        "{\"type\":\"xml\",\"mode\":\"elements\",\"recordPath\":\"/Events/Event\"}",
        "{\"type\":\"otlp\",\"signal\":\"logs\"}",
        "{\"type\":\"parquet\"}",
        "{\"type\":\"text\",\"framing\":{\"type\":\"whole\"}}",
        "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"a\",\"maxLines\":1}}",
        "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"continuePattern\":\"a\"}}"
      })
  void preservesAcceptedFormatConfigurations(String format) throws Exception {
    var config = OBJECT_MAPPER.readValue("{\"format\":" + format + "}", DecodingConfig.class);
    assertDoesNotThrow(() -> SourceDecoding.validate(config));
  }

  @Test
  void attachesTheDecodingPrefixOnceAndPreservesCompressionOrder() throws Exception {
    assertIssue(() -> DecodingConfigCompiler.compile(null), ConfigurationPath.root(), "required");
    assertIssue(
        () -> SourceDecoding.validate(new DecodingConfig(null, null)),
        of("decoding", "format"),
        "required");
    assertIssue(() -> new SourceDecoding(null).decodingConfig(), of("decoding"), "required");
    var config =
        OBJECT_MAPPER.readValue(
            "{\"compression\":[\"gzip\",null],\"format\":null}", DecodingConfig.class);
    assertIssue(
        () -> SourceDecoding.validate(config), of("decoding", "compression").index(1), "null");
    assertIssue(
        () ->
            SourceDecoding.validate(
                new DecodingConfig(
                    List.of(DecompressionCodec.AUTO, DecompressionCodec.GZIP), null)),
        of("decoding", "compression"),
        "only entry");
    assertIssue(
        () ->
            SourceDecoding.validate(
                new DecodingConfig(List.of(DecompressionCodec.GZIP), new ParquetFormatConfig())),
        of("decoding", "compression"),
        "outer compression");
    assertDoesNotThrow(
        () ->
            SourceDecoding.validate(
                new DecodingConfig(
                    List.of(DecompressionCodec.AUTO),
                    new JsonFormatConfig(JsonFormatConfig.Mode.LINES))));
    assertDoesNotThrow(
        () ->
            SourceDecoding.validate(
                new DecodingConfig(
                    List.of(DecompressionCodec.GZIP, DecompressionCodec.ZSTD),
                    new JsonFormatConfig(JsonFormatConfig.Mode.LINES))));
  }

  @Test
  void identifiesOnlyCsvCharactersParticipatingInAConflict() throws Exception {
    var config =
        OBJECT_MAPPER.readValue(
            "{\"format\":{\"type\":\"csv\",\"escape\":\"#\",\"commentMarker\":\"#\"}}",
            DecodingConfig.class);
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> SourceDecoding.validate(config));
    assertEquals(
        List.of(of("decoding", "format", "escape"), of("decoding", "format", "commentMarker")),
        failure.issues().stream().map(ConfigurationIssue::path).toList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", ",\"startPattern\":\"a\",\"continuePattern\":\"b\""})
  void identifiesBothFieldsOfTheExclusivePatternConstraint(String patterns) throws Exception {
    var config =
        OBJECT_MAPPER.readValue(
            "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\"" + patterns + "}}}",
            DecodingConfig.class);
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> SourceDecoding.validate(config));
    assertEquals(
        List.of(
            of("decoding", "format", "framing", "startPattern"),
            of("decoding", "format", "framing", "continuePattern")),
        failure.issues().stream().map(ConfigurationIssue::path).toList());
    assertTrue(
        failure.issues().stream().allMatch(issue -> issue.message().contains("exactly one")));
  }

  @Test
  void identifiesMissingProtobufFieldsIndividually() {
    for (boolean missingDescriptor : List.of(true, false)) {
      var format =
          new ProtobufFormatConfig(
              missingDescriptor ? null : "",
              missingDescriptor ? "Event" : null,
              ProtobufFormatConfig.Framing.SINGLE);
      assertIssue(
          () -> DecodingConfigCompiler.compile(new DecodingConfig(null, format)),
          of("format", missingDescriptor ? "descriptorSet" : "messageType"),
          "required");
    }
    var both = new ProtobufFormatConfig(null, null, ProtobufFormatConfig.Framing.SINGLE);
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> DecodingConfigCompiler.compile(new DecodingConfig(null, both)));
    assertEquals(
        List.of(of("format", "descriptorSet"), of("format", "messageType")),
        failure.issues().stream().map(ConfigurationIssue::path).toList());
  }

  @Test
  void identifiesMissingAmbiguousAndConflictingProtobufDefinitions() {
    var event = DescriptorProto.newBuilder().setName("Event").build();
    var file = FileDescriptorProto.newBuilder().setName("one.proto").addMessageType(event).build();
    var valid = descriptors(file);
    assertDoesNotThrow(() -> compileProtobuf(valid, "Event"));
    assertDoesNotThrow(() -> compileProtobuf(valid, ".Event"));
    assertIssue(
        () -> compileProtobuf(valid, "do-not-copy-this"),
        of("format", "messageType"),
        "defined in descriptorSet");
    assertIssue(
        () ->
            compileProtobuf(
                descriptors(file, file.toBuilder().setName("two.proto").build()), "Event"),
        of("format", "messageType"),
        "exactly one");
    assertIssue(
        () ->
            compileProtobuf(
                descriptors(file, file.toBuilder().setPackage("other").build()), "Event"),
        of("format", "descriptorSet"),
        "identical definitions");
  }

  @Test
  void identifiesMissingAndCyclicProtobufDependencies() {
    var event = DescriptorProto.newBuilder().setName("Event").build();
    var file = FileDescriptorProto.newBuilder().setName("one.proto").addMessageType(event).build();
    assertIssue(
        () ->
            compileProtobuf(
                descriptors(file.toBuilder().addDependency("missing.proto").build()), "Event"),
        of("format", "descriptorSet"),
        "all imported");
    var cyclicA = file.toBuilder().addDependency("two.proto").build();
    var cyclicB = file.toBuilder().setName("two.proto").addDependency("one.proto").build();
    assertIssue(
        () -> compileProtobuf(descriptors(cyclicA, cyclicB), "Event"),
        of("format", "descriptorSet"),
        "cycle");
  }

  @Test
  void identifiesInvalidProtobufFieldsAndMapEntryMessages() {
    var event = DescriptorProto.newBuilder().setName("Event").build();
    var file = FileDescriptorProto.newBuilder().setName("one.proto").addMessageType(event).build();
    var invalidMessage =
        event.toBuilder()
            .addField(
                FieldDescriptorProto.newBuilder()
                    .setName("bad")
                    .setNumber(0)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .build();
    assertIssue(
        () ->
            compileProtobuf(
                descriptors(
                    file.toBuilder().clearMessageType().addMessageType(invalidMessage).build()),
                "Event"),
        of("format", "descriptorSet"),
        "Invalid Protobuf");
    var mapEntry =
        event.toBuilder()
            .setOptions(MessageOptions.newBuilder().setMapEntry(true))
            .addField(
                FieldDescriptorProto.newBuilder()
                    .setName("key")
                    .setNumber(1)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(
                FieldDescriptorProto.newBuilder()
                    .setName("value")
                    .setNumber(2)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .build();
    assertIssue(
        () ->
            compileProtobuf(
                descriptors(file.toBuilder().clearMessageType().addMessageType(mapEntry).build()),
                "Event"),
        of("format", "messageType"),
        "map entry");
  }

  private static String descriptors(FileDescriptorProto... files) {
    return Base64.getEncoder()
        .encodeToString(
            FileDescriptorSet.newBuilder().addAllFile(List.of(files)).build().toByteArray());
  }

  private static void compileProtobuf(String descriptor, String message) {
    DecodingConfigCompiler.compile(
        new DecodingConfig(
            null,
            new ProtobufFormatConfig(descriptor, message, ProtobufFormatConfig.Framing.SINGLE)));
  }

  private static void assertIssue(Runnable validation, ConfigurationPath path, String constraint) {
    var failure = assertThrows(ConfigurationValidationException.class, validation::run);
    assertEquals(List.of(path), failure.issues().stream().map(ConfigurationIssue::path).toList());
    assertTrue(
        failure.issues().getFirst().message().contains(constraint), failure.issues().toString());
    assertFalse(failure.issues().getFirst().message().contains("do-not-copy-this"));
    assertEquals(IllegalArgumentException.class, failure.originalFailure().getClass());
    assertSame(failure.originalFailure(), failure.getCause());
    assertEquals(failure.originalFailure().getMessage(), failure.getMessage());
  }
}
