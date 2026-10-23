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
package io.fleak.zephflow.lib.commands.s3filereader;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_S3_FILE_READER;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.basicCommandMetricTags;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Mid-DAG command. Reads an {@code s3://bucket/key} path from a configured field of the incoming
 * record, downloads + (auto-)gunzips the object, and emits one output record per line.
 *
 * <p>Memory note: the whole file's lines are materialized into a {@code List} before returning.
 * Suitable for the small (~tens of KB gzipped) objects this is designed for; streaming/chunked
 * emission is intentionally out of scope.
 */
@Slf4j
public class S3FileReaderCommand extends ScalarCommand {

  protected S3FileReaderCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter inputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter outputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter =
        metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);

    S3FileReaderDto.Config config = (S3FileReaderDto.Config) commandConfig;

    UsernamePasswordCredential cred =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId()).orElse(null);
    if (config.getCredentialId() != null && !config.getCredentialId().isBlank() && cred == null) {
      throw new IllegalStateException(
          "S3 credentialId '"
              + config.getCredentialId()
              + "' was configured but could not be resolved in JobContext");
    }
    String accessKeyId = cred != null ? cred.getUsername() : null;
    String secretAccessKey = cred != null ? cred.getPassword() : null;
    S3BackendConfig backendConfig =
        new S3BackendConfig(
            config.getRegion(), accessKeyId, secretAccessKey, config.getS3EndpointOverride());
    S3Client s3Client = S3Backend.client(backendConfig);

    RecordDecoder decoder = null;
    if (config.getEmission() == S3FileReaderDto.Emission.DESERIALIZE) {
      decoder = DecodingConfigCompiler.compile(decodingConfig(config.getEncodingType()));
    }

    return new S3FileReaderExecutionContext(
        inputMessageCounter, outputMessageCounter, errorCounter, config, s3Client, decoder);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) throws Exception {
    S3FileReaderExecutionContext ctx = (S3FileReaderExecutionContext) context;
    Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
    ctx.getInputMessageCounter().increase(tags);
    try {
      S3FileReaderDto.Config config = ctx.getConfig();

      FleakData pathData = event.getPayload().get(config.getPathField());
      if (pathData == null || !(pathData.unwrap() instanceof String pathStr) || pathStr.isBlank()) {
        throw new IllegalArgumentException(
            "pathField '"
                + config.getPathField()
                + "' not found or not a non-blank string in record");
      }

      S3Path s3Path = parseS3Path(pathStr, config.isUrlDecodeKey());
      boolean gunzip = shouldGunzip(config.getCompression(), s3Path.key());

      GetObjectRequest request =
          GetObjectRequest.builder().bucket(s3Path.bucket()).key(s3Path.key()).build();

      List<RecordFleakData> output = new ArrayList<>();
      try (InputStream raw = ctx.getS3Client().getObject(request);
          InputStream in = gunzip ? new GZIPInputStream(raw) : raw;
          BufferedReader br =
              new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        int lineNumber = 0;
        while ((line = br.readLine()) != null) {
          lineNumber++;
          if (config.getEmission() == S3FileReaderDto.Emission.LINE) {
            output.add((RecordFleakData) FleakData.wrap(Map.of("line", line, "file", pathStr)));
          } else {
            output.addAll(
                decodeLine(ctx.getDecoder(), config.getEncodingType(), line, pathStr, lineNumber));
          }
        }
      }
      ctx.getOutputMessageCounter().increase(output.size(), tags);
      return output;
    } catch (Exception e) {
      ctx.getErrorCounter().increase(tags);
      throw e;
    }
  }

  /** PARQUET and KV keep the message the legacy deserializer factory threw. */
  private static DecodingConfig decodingConfig(EncodingType encodingType) {
    if (encodingType == EncodingType.PARQUET || encodingType == EncodingType.KV) {
      throw new UnsupportedOperationException(
          encodingType + " is a sink-only format (not supported for deserialization)");
    }
    return MessageDecodingConverter.decodingConfig(encodingType, null, false);
  }

  private static List<RecordFleakData> decodeLine(
      RecordDecoder decoder, EncodingType encodingType, String line, String path, int lineNumber) {
    if (encodingType == EncodingType.TEXT && line.isEmpty()) {
      return List.of((RecordFleakData) FleakData.wrap(Map.of(FIELD_NAME_RAW, "")));
    }
    DecodeResult result;
    try {
      result = decoder.decodeAll(line.getBytes(StandardCharsets.UTF_8), DecodeLimits.UNLIMITED);
    } catch (RuntimeException e) {
      throw lineFailure("READ_FAILURE", e, "", encodingType, path, lineNumber);
    }
    if (result.problems().isEmpty()) {
      return result.records();
    }
    DecodeOutcome first = result.problems().getFirst();
    String location = MessageDecodingConverter.location(first, result.problems().size() - 1);
    throw switch (first) {
      case DecodeOutcome.Failed failed ->
          lineFailure(
              failed.kind().name(), failed.error(), location, encodingType, path, lineNumber);
      default ->
          lineFailure(
              "READ_FAILURE",
              new IllegalStateException(
                  "unexpected " + first.getClass().getSimpleName() + " outcome"),
              location,
              encodingType,
              path,
              lineNumber);
    };
  }

  private static IllegalArgumentException lineFailure(
      String kind,
      Exception error,
      String location,
      EncodingType encodingType,
      String path,
      int lineNumber) {
    String format = SourceDecoding.formatName(decodingConfig(encodingType).format());
    Throwable root = ExceptionUtils.getThrowableList(error).getLast();
    String firstLine =
        root.getMessage() == null ? "" : root.getMessage().lines().findFirst().orElse("");
    String detail = firstLine.isBlank() ? root.getClass().getSimpleName() : firstLine;
    String message =
        kind
            + " "
            + format
            + " line: "
            + UnitRun.causeName(error)
            + "\nAction: check that each line of the object is what the node's encodingType "
            + encodingType
            + " expects, or fix the object; the input record failed and no record of this object"
            + " was emitted\n"
            + detail
            + "\n"
            + path
            + " line "
            + lineNumber
            + (location.isEmpty() ? "" : ", " + location);
    log.warn(message, error);
    return new IllegalArgumentException(message, error);
  }

  static S3Path parseS3Path(String pathStr, boolean urlDecodeKey) {
    if (!pathStr.startsWith("s3://")) {
      throw new IllegalArgumentException("path '" + pathStr + "' is not an s3:// URI");
    }
    String stripped = pathStr.substring("s3://".length());
    int slash = stripped.indexOf('/');
    if (slash < 0) {
      throw new IllegalArgumentException("path '" + pathStr + "' has no object key");
    }
    String bucket = stripped.substring(0, slash);
    String key = stripped.substring(slash + 1);
    if (bucket.isBlank() || key.isBlank()) {
      throw new IllegalArgumentException("path '" + pathStr + "' has a blank bucket or key");
    }
    if (urlDecodeKey) {
      key = URLDecoder.decode(key, StandardCharsets.UTF_8);
    }
    return new S3Path(bucket, key);
  }

  static boolean shouldGunzip(S3FileReaderDto.Compression compression, String key) {
    return switch (compression) {
      case GZIP -> true;
      case NONE -> false;
      case AUTO -> key.endsWith(".gz");
    };
  }

  record S3Path(String bucket, String key) {}

  @Override
  public String commandName() {
    return COMMAND_NAME_S3_FILE_READER;
  }
}
