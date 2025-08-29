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
package io.fleak.zephflow.lib.utils;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.apache.commons.text.StringEscapeUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public interface MiscUtils {

  String FIELD_NAME_RAW = "__raw__";
  String FIELD_NAME_TS = "__ts__";

  String METADATA_PREFIX = "__md_";
  String METADATA_KEY = METADATA_PREFIX + "key";
  String METADATA_KAFKA_PREFIX = METADATA_PREFIX + "kafka_";
  String METADATA_KAFKA_TOPIC = METADATA_KAFKA_PREFIX + "topic";
  String METADATA_KAFKA_PARTITION = METADATA_KAFKA_PREFIX + "partition";
  String METADATA_KAFKA_OFFSET = METADATA_KAFKA_PREFIX + "offset";
  String METADATA_KAFKA_TIMESTAMP = METADATA_KAFKA_PREFIX + "timestamp";
  String METADATA_KAFKA_TIMESTAMP_TYPE = METADATA_KAFKA_PREFIX + "timestamp_type";
  String METADATA_KAFKA_SERIALIZED_KEY_SIZE = METADATA_KAFKA_PREFIX + "serialized_key_size";
  String METADATA_KAFKA_SERIALIZED_VALUE_SIZE = METADATA_KAFKA_PREFIX + "serialized_value_size ";
  String METADATA_KAFKA_LEADER_EPOCH = METADATA_KAFKA_PREFIX + "leader_epoch ";
  String METADATA_KAFKA_HEADER_PREFIX = METADATA_KAFKA_PREFIX + "header_";

  String METADATA_KINESIS_PARTITION_KEY = "kinesis_partition_key";
  String METADATA_KINESIS_SEQUENCE_NUMBER = "kinesis_sequence_number";
  String METADATA_KINESIS_HASH_KEY = "kinesis_hash_key";
  String METADATA_KINESIS_SCHEMA_DATA_FORMAT = "kinesis_schema_data_format";
  String METADATA_KINESIS_SCHEMA_DEFINITION = "kinesis_schema_definition";
  String METADATA_KINESIS_SCHEMA_NAME = "kinesis_schema_name";

  String COMMAND_NAME_NOOP = "noop";
  String COMMAND_NAME_SQL_EVAL = "sqleval";

  String COMMAND_NAME_S3_SINK = "s3sink";
  String COMMAND_NAME_KINESIS_SOURCE = "kinesissource";
  String COMMAND_NAME_KINESIS_SINK = "kinesissink";
  String COMMAND_NAME_KAFKA_SOURCE = "kafkasource";
  String COMMAND_NAME_KAFKA_SINK = "kafkasink";
  String COMMAND_NAME_EVAL = "eval";
  String COMMAND_NAME_ASSERTION = "assertion";
  String COMMAND_NAME_FILTER = "filter";
  String COMMAND_NAME_STDIN = "stdin";
  String COMMAND_NAME_STDOUT = "stdout";
  String COMMAND_NAME_PARSER = "parser";
  String COMMAND_NAME_FILE_SOURCE = "filesource";
  String COMMAND_NAME_READER_SOURCE = "reader";

  String COMMAND_NAME_CLICK_HOUSE_SINK = "clickhousesink";
  String COMMAND_NAME_DELTA_LAKE_SINK = "deltalakesink";
  String METRIC_NAME_INPUT_EVENT_COUNT = "input_event_count";
  String METRIC_NAME_INPUT_EVENT_SIZE_COUNT = "input_event_size";
  String METRIC_NAME_INPUT_DESER_ERR_COUNT = "input_deser_err_count";
  String METRIC_NAME_OUTPUT_EVENT_COUNT = "output_event_count";
  String METRIC_NAME_OUTPUT_EVENT_SIZE_COUNT = "output_event_size";
  String METRIC_NAME_ERROR_EVENT_COUNT = "error_event_count";
  String METRIC_NAME_SINK_OUTPUT_COUNT = "sink_output_count";
  String METRIC_NAME_SINK_ERROR_COUNT = "sink_error_count";

  String EVENT_TAG_FIELD = "__tag__";
  String METRIC_TAG_CALLING_USER = "calling_user";
  String METRIC_TAG_COMMAND_NAME = "command_name";
  String METRIC_TAG_NODE_ID = "node_id";
  String METRIC_TAG_ENV = "env";
  String METRIC_TAG_SERVICE = "service";
  String REGEX_WINDOWS_LINE_SEPARATOR = "\\r\\n";
  String REGEX_LINUX_LINE_SEPARATOR = "\n";

  String ROOT_OBJECT_VARIABLE_NAME = "$";

  String FLAG_ENFORCE_CREDENTIALS = "enforce_cred";

  static boolean enforceCredentials(JobContext jobContext) {
    var val = jobContext.getOtherProperties().getOrDefault(FLAG_ENFORCE_CREDENTIALS, null);
    return val instanceof Boolean && (Boolean) val;
  }

  static boolean isMetadataField(String fieldName) {
    return fieldName.startsWith(METADATA_PREFIX);
  }

  static <T> T getFirstOneValueFromMapEnsureAtLeastOne(Map<?, T> map) {
    Preconditions.checkArgument(MapUtils.isNotEmpty(map), "The map is empty");
    return map.values().iterator().next();
  }

  static <T> T getOneValueFromMapEnsureExactlyOne(Map<?, T> map) {
    Preconditions.checkArgument(
        MapUtils.isNotEmpty(map)
            && (map.size() == 1 && new ArrayList<>(map.values()).get(0) != null),
        "The map doesn't contain exactly one entry");
    return new ArrayList<>(map.values()).get(0);
  }

  static <T> T getOneValueFromCollectionEnsureExactlyOne(Collection<T> c) {
    Preconditions.checkArgument(
        CollectionUtils.size(c) == 1,
        "Exactly one value expected in the collection. collection: %s",
        c);
    return c.iterator().next();
  }

  static <T> Optional<T> getOneValueFromCollectionEnsureAtMostOne(Collection<T> c) {
    Preconditions.checkArgument(
        CollectionUtils.size(c) <= 1, "input collection contains more than one entries");

    return CollectionUtils.isEmpty(c) ? Optional.empty() : Optional.of(c.iterator().next());
  }

  static <IN, OUT> OUT nullOrCompute(IN input, Function<IN, OUT> func) {
    if (input == null) {
      return null;
    }
    return func.apply(input);
  }

  static String loadStringFromResource(String resourceName) {
    try (InputStream in = MiscUtils.class.getResourceAsStream(resourceName)) {
      Preconditions.checkNotNull(in);
      return IOUtils.toString(in, Charset.defaultCharset())
          .replaceAll(REGEX_WINDOWS_LINE_SEPARATOR, REGEX_LINUX_LINE_SEPARATOR);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String toBase64String(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(bytes);
  }

  static byte[] fromBase64String(String base64String) {
    return Base64.getDecoder().decode(base64String);
  }

  static <T> T getRequiredCommandArgValue(
      CommandLine cmd, String argName, Function<String, T> func) {
    String value = cmd.getOptionValue(argName);
    Preconditions.checkNotNull(value);
    return func.apply(value);
  }

  static <T> T getOptionalCommandArgValue(
      CommandLine cmd, String argName, Function<String, T> func, T defaultValue) {
    if (!cmd.hasOption(argName)) {
      return defaultValue;
    }
    String value = cmd.getOptionValue(argName);
    return func.apply(value);
  }

  static <T> boolean validArrayIndex(List<T> array, int index) {
    return index >= 0 && index < array.size();
  }

  static <E extends RuntimeException> void ensureConditionOtherwiseThrow(
      boolean condition, Supplier<E> eSupplier) {
    if (condition) {
      return;
    }
    throw eSupplier.get();
  }

  static void ensureFileExists(File file) {
    if (file.exists()) {
      return;
    }
    try {
      boolean fileCreated = file.createNewFile();
      Preconditions.checkArgument(
          fileCreated, "File already exists but file.exists() returns false");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static void threadSleep(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      // no-op
    }
  }

  static List<ParseTree> collectDelimitedTreeElements(ParserRuleContext ctx) {
    return collectDelimitedTreeElements(ctx, 0, 0);
  }

  static List<ParseTree> collectDelimitedTreeElements(
      ParserRuleContext ctx, int startOffset, int endOffset) {
    List<ParseTree> children = new ArrayList<>();
    for (int i = startOffset; i < ctx.getChildCount() - endOffset; i += 2) {
      children.add(ctx.getChild(i));
    }
    return children;
  }

  // strip quotes and unescape content
  static String unescapeStrLiteral(String strLiteralText) {
    String unquoted = strLiteralText.substring(1, strLiteralText.length() - 1);
    return unescapeJson(unquoted);
  }

  // wrap with double quotes and escape content
  static String escapeStrLiteral(String strLiteral) {
    return String.format("\"%s\"", escapeJson(strLiteral));
  }

  static String generateRandomHash() {
    return new BigInteger(128, new SecureRandom()).toString(32).substring(0, 16);
  }

  static Map<String, String> basicCommandMetricTags(
      Map<String, String> pipelineTags, String commandName, String currentNodeId) {
    Map<String, String> metricTags = new HashMap<>(pipelineTags);
    validateMetricTags(metricTags);
    metricTags.put(METRIC_TAG_COMMAND_NAME, commandName);
    metricTags.put(METRIC_TAG_NODE_ID, currentNodeId);
    return metricTags;
  }

  static void validateMetricTags(Map<String, String> metricTags) {
    Preconditions.checkNotNull(
        metricTags.get(METRIC_TAG_SERVICE), "metric tag missing service name");
    Preconditions.checkNotNull(metricTags.get(METRIC_TAG_ENV), "metric tag missing environment");
  }

  static Map<String, String> getCallingUserTagAndEventTags(
      String nullableUserId, RecordFleakData event) {
    Map<String, String> tags =
        Optional.ofNullable(nullableUserId)
            .map(u -> new HashMap<>(Map.of(METRIC_TAG_CALLING_USER, u)))
            .orElse(new HashMap<>());

    if (event == null) return tags;

    FleakData tagData = event.getPayload().get(EVENT_TAG_FIELD);
    if (tagData == null) return tags;

    Object unwrapped = tagData.unwrap();
    if (!(unwrapped instanceof Map<?, ?>)) return tags;

    @SuppressWarnings("unchecked")
    Map<String, Object> tagMap = (Map<String, Object>) unwrapped;
    for (Map.Entry<String, Object> entry : tagMap.entrySet()) {
      if (entry.getValue() != null) {
        tags.put(entry.getKey(), entry.getValue().toString());
      }
    }

    return tags;
  }

  static <T> T lookupFromMapOrThrow(
      Map<String, Serializable> map, @NonNull String key, Class<T> clz) {
    Object rawValue = MapUtils.getObject(map, key);
    Preconditions.checkNotNull(rawValue, "no value found for key: %s", key);
    T value = OBJECT_MAPPER.convertValue(rawValue, clz);
    Preconditions.checkNotNull(value);
    return value;
  }

  static Optional<UsernamePasswordCredential> lookupUsernamePasswordCredentialOpt(
      JobContext jobContext, String credentialId) {
    try {
      var k = lookupUsernamePasswordCredential(jobContext, credentialId);
      return StringUtils.isEmpty(k.getUsername()) ? Optional.empty() : Optional.of(k);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  static UsernamePasswordCredential lookupUsernamePasswordCredential(
      JobContext jobContext, String credentialId) {
    Preconditions.checkNotNull(credentialId, "credentialId not provided");
    try {
      return lookupFromMapOrThrow(
          jobContext.getOtherProperties(), credentialId, UsernamePasswordCredential.class);
    } catch (Exception e) {
      throw new RuntimeException(
          "failed to load username password credential for credentialId: " + credentialId, e);
    }
  }

  /** Helper method to look up ApiKeyCredential from JobContext */
  static Optional<ApiKeyCredential> lookupApiKeyCredentialOpt(
      JobContext jobContext, String credentialId) {
    try {
      var credential = lookupApiKeyCredential(jobContext, credentialId);
      return StringUtils.isEmpty(credential.getKey()) ? Optional.empty() : Optional.of(credential);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  static ApiKeyCredential lookupApiKeyCredential(JobContext jobContext, String credentialId) {
    Preconditions.checkNotNull(credentialId, "credentialId not provided");
    try {
      return lookupFromMapOrThrow(
          jobContext.getOtherProperties(), credentialId, ApiKeyCredential.class);
    } catch (Exception e) {
      throw new RuntimeException(
          "failed to load API key credential for credentialId: " + credentialId, e);
    }
  }

  /** Helper method to look up GcpCredential from JobContext */
  static Optional<GcpCredential> lookupGcpCredentialOpt(
      JobContext jobContext, String credentialId) {
    try {
      var credential = lookupGcpCredential(jobContext, credentialId);
      return Optional.of(credential);
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  static GcpCredential lookupGcpCredential(JobContext jobContext, String credentialId) {
    Preconditions.checkNotNull(credentialId, "credentialId not provided");
    try {
      return lookupFromMapOrThrow(
          jobContext.getOtherProperties(), credentialId, GcpCredential.class);
    } catch (Exception e) {
      throw new RuntimeException(
          "failed to load GCP credential for credentialId: " + credentialId, e);
    }
  }

  static <T extends Enum<T>> T parseEnum(@NonNull Class<T> enumType, @NonNull String enumStr) {
    try {
      return Enum.valueOf(enumType, enumStr);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for enum " + enumType.getSimpleName() + ": " + enumStr, e);
    }
  }

  static String normalizeStrLiteral(String strLiteralText) {
    String unquoted = strLiteralText.substring(1, strLiteralText.length() - 1);
    return unescapeJava(unquoted);
  }
}
