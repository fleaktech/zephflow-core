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
package io.fleak.zephflow.lib.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.commands.elasticsearchsource.ElasticsearchSourceDto;
import io.fleak.zephflow.lib.commands.kinesissource.KinesisSourceDto;
import io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkDto;
import io.fleak.zephflow.lib.commands.s3.S3SinkDto;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3RealtimeSourceDto;
import io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkDto;
import io.fleak.zephflow.lib.commands.sqssink.SqsSinkDto;
import io.fleak.zephflow.lib.commands.zerobussink.ZerobusSinkDto;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonConfigParserTest {

  @Test
  void parseConfig_withNullBatchSize_usesDefaultValue() {
    var parser = new JsonConfigParser<>(S3SinkDto.Config.class);

    Map<String, Object> config = new HashMap<>();
    config.put("regionStr", "us-west-2");
    config.put("bucketName", "test-bucket");
    config.put("keyName", "test-key");
    config.put("encoding", Map.of("format", Map.of("type", "json", "mode", "lines")));
    config.put("batchSize", null);

    var result = parser.parseConfig(config);

    assertEquals(10_000, result.getBatchSize());
  }

  @Test
  void parseConfig_withExplicitBatchSize_usesProvidedValue() {
    var parser = new JsonConfigParser<>(S3SinkDto.Config.class);

    Map<String, Object> config = new HashMap<>();
    config.put("regionStr", "us-west-2");
    config.put("bucketName", "test-bucket");
    config.put("keyName", "test-key");
    config.put("encoding", Map.of("format", Map.of("type", "json", "mode", "lines")));
    config.put("batchSize", 5000);

    var result = parser.parseConfig(config);

    assertEquals(5000, result.getBatchSize());
  }

  @Test
  void parseConfig_withOmittedBatchSize_usesDefaultValue() {
    var parser = new JsonConfigParser<>(S3SinkDto.Config.class);

    Map<String, Object> config = new HashMap<>();
    config.put("regionStr", "us-west-2");
    config.put("bucketName", "test-bucket");
    config.put("keyName", "test-key");
    config.put("encoding", Map.of("format", Map.of("type", "json", "mode", "lines")));

    var result = parser.parseConfig(config);

    assertEquals(10_000, result.getBatchSize());
  }

  static Stream<Arguments> removedFormatKeys() {
    return Stream.of(
        Arguments.of(ElasticsearchSourceDto.Config.class, "encodingType", "JSON_OBJECT"),
        Arguments.of(KinesisSourceDto.Config.class, "compressionTypes", List.of("GZIP")),
        Arguments.of(S3RealtimeSourceDto.Config.class, "compressionType", "GZIP"),
        Arguments.of(S3SinkDto.Config.class, "avroSchema", Map.of()),
        Arguments.of(S3SinkDto.Config.class, "kvFields", List.of("a")),
        Arguments.of(SplunkHecSinkDto.Config.class, "csvColumns", List.of("a")),
        Arguments.of(SqsSinkDto.Config.class, "encodingType", "JSON_OBJECT"),
        Arguments.of(PubSubSinkDto.Config.class, "encodingType", "JSON_OBJECT"),
        Arguments.of(ZerobusSinkDto.Config.class, "encodingType", "protobuf"));
  }

  @ParameterizedTest
  @MethodSource("removedFormatKeys")
  <T extends CommandConfig> void aRemovedFormatKeyIsAParseErrorNamingIt(
      Class<T> configClass, String key, Object value) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new JsonConfigParser<>(configClass).parseConfig(Map.of(key, value)));

    assertEquals(
        "Unrecognized field \"" + key + "\"",
        e.getMessage().substring(0, e.getMessage().indexOf('"', 20) + 1));
  }
}
