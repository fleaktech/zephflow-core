# `s3filereader` Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a mid-DAG `s3filereader` command that reads an `s3://bucket/key` path from a configurable field of the incoming record, downloads + (auto-)gunzips the object, and fans the record out into one output record per line.

**Architecture:** A `ScalarCommand` (like `parser`) whose `processOneEvent` returns `List<RecordFleakData>` — one input record → N output lines. The `S3Client` is built once in `createExecutionContext` (reusing `S3Backend.client`) and reused across events. Per-event failures throw and are converted to failure events by the `ScalarCommand.process` base loop.

**Tech Stack:** Java 17, AWS SDK v2 (`software.amazon.awssdk:s3`, already a dep), Lombok, Jackson (`JsonConfigParser`), JUnit 5 + Testcontainers LocalStack.

## Global Constraints

- **No new dependencies.** `software.amazon.awssdk:s3` and `libs.testcontainers.localstack` are already wired into `lib`.
- **Mirror the `parser` command package layout.** New package: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/`.
- **Apache license header** on every new `.java` file (copy verbatim from any existing file in the repo, e.g. `ParserCommand.java` lines 1–13).
- **`./gradlew :lib:spotlessApply` must run clean** before every commit; CI runs `spotlessJavaCheck`. Run `spotlessApply` as the last step before each commit.
- **Verified fact:** Lombok 1.18.34 in this repo *does* apply `@Builder.Default` values through `JsonConfigParser` (Jackson no-arg-constructor path). So `compression=AUTO` / `emission=LINE` / `urlDecodeKey=true` / `batchSize=500` defaults apply even when omitted from the config JSON. Mirror the `ReaderDto` / `FsSourceDto` annotation set exactly.
- **Integration tests** that need LocalStack must be annotated `@Tag("integration")` and `@Testcontainers`, mirroring `S3BackendIntegrationTest`.
- Counters: increment input once per event, output by `output.size()`, error once on any exception — using the `getCallingUserTagAndEventTags` / `basicCommandMetricTags` pattern from `ParserCommand`.

---

### Task 1: Config DTO + command-name constant

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderDto.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` (add constant after line 88, near the other `COMMAND_NAME_*`)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigParseTest.java`

**Interfaces:**
- Produces: `S3FileReaderDto.Config implements CommandConfig` with getters `getPathField()`, `getCredentialId()`, `getRegion()`, `getS3EndpointOverride()`, `getCompression()`, `getEmission()`, `getEncodingType()`, `isUrlDecodeKey()`, `getBatchSize()`; nested enums `S3FileReaderDto.Compression { AUTO, NONE, GZIP }` and `S3FileReaderDto.Emission { LINE, DESERIALIZE }`.
- Produces: `MiscUtils.COMMAND_NAME_S3_FILE_READER = "s3filereader"`.

- [ ] **Step 1: Add the command-name constant**

In `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`, after the line `String COMMAND_NAME_FS_SOURCE = "fssource";` (line 86) add:

```java
  String COMMAND_NAME_S3_FILE_READER = "s3filereader";
```

- [ ] **Step 2: Write the DTO**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderDto.java` (prepend the Apache license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface S3FileReaderDto {

  enum Compression {
    AUTO,
    NONE,
    GZIP
  }

  enum Emission {
    LINE,
    DESERIALIZE
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  class Config implements CommandConfig {
    private String pathField;
    private String credentialId;
    private String region;
    private String s3EndpointOverride;
    @Builder.Default private Compression compression = Compression.AUTO;
    @Builder.Default private Emission emission = Emission.LINE;
    @Builder.Default private EncodingType encodingType = EncodingType.JSON_OBJECT_LINE;
    @Builder.Default private boolean urlDecodeKey = true;
    @Builder.Default private int batchSize = 500;
  }
}
```

- [ ] **Step 3: Write the failing parse test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigParseTest.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class S3FileReaderConfigParseTest {

  @Test
  void minimalConfigAppliesDefaults() {
    var parser = new JsonConfigParser<>(S3FileReaderDto.Config.class);
    S3FileReaderDto.Config c =
        parser.parseConfig(Map.of("pathField", "s3Path", "region", "us-east-1"));

    assertEquals("s3Path", c.getPathField());
    assertEquals("us-east-1", c.getRegion());
    assertEquals(S3FileReaderDto.Compression.AUTO, c.getCompression());
    assertEquals(S3FileReaderDto.Emission.LINE, c.getEmission());
    assertEquals(EncodingType.JSON_OBJECT_LINE, c.getEncodingType());
    assertTrue(c.isUrlDecodeKey());
    assertEquals(500, c.getBatchSize());
    assertNull(c.getCredentialId());
  }

  @Test
  void explicitValuesOverrideDefaults() {
    var parser = new JsonConfigParser<>(S3FileReaderDto.Config.class);
    S3FileReaderDto.Config c =
        parser.parseConfig(
            Map.of(
                "pathField", "p",
                "region", "us-west-2",
                "compression", "NONE",
                "emission", "DESERIALIZE",
                "urlDecodeKey", false));

    assertEquals(S3FileReaderDto.Compression.NONE, c.getCompression());
    assertEquals(S3FileReaderDto.Emission.DESERIALIZE, c.getEmission());
    assertFalse(c.isUrlDecodeKey());
  }
}
```

- [ ] **Step 4: Run the test**

Run: `./gradlew :lib:spotlessApply && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderConfigParseTest"`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderDto.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigParseTest.java
git commit -m "feat(s3filereader): add config DTO and command-name constant"
```

---

### Task 2: Config validator

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigValidator.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigValidatorTest.java`

**Interfaces:**
- Consumes: `S3FileReaderDto.Config` (Task 1).
- Produces: `S3FileReaderConfigValidator implements ConfigValidator` with `void validateConfig(CommandConfig, String nodeId, JobContext)`.

Validator rules (from spec §6): `pathField` non-blank; `region` non-blank; if `emission == DESERIALIZE` then `encodingType` must be set. (`compression`/`emission` enum-value validity is enforced by Jackson at parse time.)

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigValidatorTest.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class S3FileReaderConfigValidatorTest {

  private final S3FileReaderConfigValidator validator = new S3FileReaderConfigValidator();

  @Test
  void acceptsValidLineConfig() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder().pathField("s3Path").region("us-east-1").build();
    assertDoesNotThrow(() -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }

  @Test
  void rejectsBlankPathField() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder().pathField("  ").region("us-east-1").build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }

  @Test
  void rejectsBlankRegion() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder().pathField("s3Path").region("").build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }

  @Test
  void rejectsDeserializeWithoutEncodingType() {
    S3FileReaderDto.Config c =
        S3FileReaderDto.Config.builder()
            .pathField("s3Path")
            .region("us-east-1")
            .emission(S3FileReaderDto.Emission.DESERIALIZE)
            .encodingType(null)
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(c, "n", JOB_CONTEXT));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderConfigValidatorTest"`
Expected: FAIL — `S3FileReaderConfigValidator` does not exist (compile error).

- [ ] **Step 3: Write the validator**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigValidator.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class S3FileReaderConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    S3FileReaderDto.Config config = (S3FileReaderDto.Config) commandConfig;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getPathField()), "pathField must not be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getRegion()), "region must not be blank");
    if (config.getEmission() == S3FileReaderDto.Emission.DESERIALIZE) {
      Preconditions.checkArgument(
          config.getEncodingType() != null,
          "encodingType is required when emission is DESERIALIZE");
    }
  }
}
```

Note: `Preconditions.checkArgument` throws `IllegalArgumentException`; `org.apache.commons.lang3.StringUtils` is already on the `lib` classpath (used throughout, e.g. `MiscUtils`).

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:spotlessApply && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderConfigValidatorTest"`
Expected: PASS (all 4 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigValidator.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderConfigValidatorTest.java
git commit -m "feat(s3filereader): add config validator"
```

---

### Task 3: Execution context

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderExecutionContext.java`

**Interfaces:**
- Consumes: `S3FileReaderDto.Config` (Task 1); `software.amazon.awssdk.services.s3.S3Client`; `io.fleak.zephflow.lib.serdes.des.FleakDeserializer`.
- Produces: `S3FileReaderExecutionContext extends DefaultExecutionContext` with getters `getConfig()`, `getS3Client()`, `getDeserializer()` (Lombok `@Value`) plus inherited `getInputMessageCounter()` / `getOutputMessageCounter()` / `getErrorCounter()`. `close()` closes the `S3Client`.

This task has no standalone unit test — it is a plain data holder verified by Task 4/5. Its deliverable is "compiles and closes the client."

- [ ] **Step 1: Write the execution context**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderExecutionContext.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import software.amazon.awssdk.services.s3.S3Client;

@EqualsAndHashCode(callSuper = true)
@Value
public class S3FileReaderExecutionContext extends DefaultExecutionContext {
  S3FileReaderDto.Config config;
  S3Client s3Client;
  FleakDeserializer<?> deserializer;

  public S3FileReaderExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      S3FileReaderDto.Config config,
      S3Client s3Client,
      FleakDeserializer<?> deserializer) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.config = config;
    this.s3Client = s3Client;
    this.deserializer = deserializer;
  }

  @Override
  public void close() throws IOException {
    if (s3Client != null) {
      s3Client.close();
    }
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `./gradlew :lib:spotlessApply && ./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderExecutionContext.java
git commit -m "feat(s3filereader): add execution context"
```

---

### Task 4: Command + factory + registry wiring

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandFactory.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` (add import + one `.put(...)` line)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandUnitTest.java`

**Interfaces:**
- Consumes: `S3FileReaderDto` (Task 1), `S3FileReaderConfigValidator` (Task 2), `S3FileReaderExecutionContext` (Task 3); `S3Backend.client(S3BackendConfig)`, `S3BackendConfig(region, accessKeyId, secretAccessKey, s3EndpointOverride)`, `MiscUtils.lookupUsernamePasswordCredentialOpt`, `DeserializerFactory.createDeserializerFactory(EncodingType).createDeserializer()`.
- Produces: `S3FileReaderCommand extends ScalarCommand`; package-visible static helpers `static S3Path parseS3Path(String pathStr, boolean urlDecodeKey)`, `static boolean shouldGunzip(S3FileReaderDto.Compression, String key)`, and `record S3Path(String bucket, String key)`. `S3FileReaderCommandFactory extends CommandFactory` whose `commandType()` returns `INTERMEDIATE_COMMAND`.

- [ ] **Step 1: Write the failing unit test (pure logic + wiring, no S3)**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandUnitTest.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import org.junit.jupiter.api.Test;

class S3FileReaderCommandUnitTest {

  @Test
  void parsesPlainPath() {
    var p = S3FileReaderCommand.parseS3Path("s3://my-bucket/a/b/c.log.gz", true);
    assertEquals("my-bucket", p.bucket());
    assertEquals("a/b/c.log.gz", p.key());
  }

  @Test
  void urlDecodesKeyOnly() {
    var p = S3FileReaderCommand.parseS3Path("s3://my-bucket/2026/03/19/foo+bar%2Fx.log.gz", true);
    assertEquals("my-bucket", p.bucket());
    assertEquals("2026/03/19/foo bar/x.log.gz", p.key());
  }

  @Test
  void skipsUrlDecodeWhenDisabled() {
    var p = S3FileReaderCommand.parseS3Path("s3://my-bucket/foo+bar.log", false);
    assertEquals("foo+bar.log", p.key());
  }

  @Test
  void rejectsNonS3Path() {
    assertThrows(
        IllegalArgumentException.class,
        () -> S3FileReaderCommand.parseS3Path("https://example.com/x", true));
  }

  @Test
  void rejectsPathWithoutKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> S3FileReaderCommand.parseS3Path("s3://only-bucket", true));
  }

  @Test
  void gunzipDecision() {
    assertTrue(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.GZIP, "x.log"));
    assertFalse(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.NONE, "x.log.gz"));
    assertTrue(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.AUTO, "x.log.gz"));
    assertFalse(S3FileReaderCommand.shouldGunzip(S3FileReaderDto.Compression.AUTO, "x.log"));
  }

  @Test
  void registeredAsIntermediateCommand() {
    var factory = OperatorCommandRegistry.OPERATOR_COMMANDS.get("s3filereader");
    assertNotNull(factory);
    assertEquals(CommandType.INTERMEDIATE_COMMAND, factory.commandType());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderCommandUnitTest"`
Expected: FAIL — `S3FileReaderCommand` does not exist (compile error).

- [ ] **Step 3: Write the command**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommand.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_S3_FILE_READER;
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
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
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

    FleakDeserializer<?> deserializer = null;
    if (config.getEmission() == S3FileReaderDto.Emission.DESERIALIZE) {
      deserializer =
          DeserializerFactory.createDeserializerFactory(config.getEncodingType())
              .createDeserializer();
    }

    return new S3FileReaderExecutionContext(
        inputMessageCounter, outputMessageCounter, errorCounter, config, s3Client, deserializer);
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
      if (pathData == null
          || !(pathData.unwrap() instanceof String pathStr)
          || pathStr.isBlank()) {
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
        while ((line = br.readLine()) != null) {
          if (config.getEmission() == S3FileReaderDto.Emission.LINE) {
            output.add((RecordFleakData) FleakData.wrap(Map.of("line", line, "file", pathStr)));
          } else {
            output.addAll(
                ctx.getDeserializer()
                    .deserialize(
                        new SerializedEvent(
                            null, line.getBytes(StandardCharsets.UTF_8), null)));
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
```

Note: `URLDecoder.decode` turns `+` into a space and `%2F` into `/`. This is the intended S3-event-notification decoding; only the key is decoded, never the bucket.

- [ ] **Step 4: Write the factory**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandFactory.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.JsonConfigParser;

public class S3FileReaderCommandFactory extends CommandFactory {
  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<S3FileReaderDto.Config> configParser =
        new JsonConfigParser<>(S3FileReaderDto.Config.class);
    var configValidator = new S3FileReaderConfigValidator();
    return new S3FileReaderCommand(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public CommandType commandType() {
    return CommandType.INTERMEDIATE_COMMAND;
  }
}
```

- [ ] **Step 5: Wire into the registry**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`:

Add the import (alphabetical, after the `reader` import on line 46):

```java
import io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderCommandFactory;
```

Add this builder line after `.put(COMMAND_NAME_FS_SOURCE, new FsSourceCommandFactory())` (line 77):

```java
          .put(COMMAND_NAME_S3_FILE_READER, new S3FileReaderCommandFactory())
```

(`COMMAND_NAME_S3_FILE_READER` resolves via the existing `import static io.fleak.zephflow.lib.utils.MiscUtils.*;` at the top of the file.)

- [ ] **Step 6: Run the unit test to verify it passes**

Run: `./gradlew :lib:spotlessApply && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderCommandUnitTest"`
Expected: PASS (all tests, including `registeredAsIntermediateCommand`).

- [ ] **Step 7: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandFactory.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandUnitTest.java
git commit -m "feat(s3filereader): add command, factory, and registry wiring"
```

---

### Task 5: LocalStack integration tests (read / gzip / url-encode / failure / deserialize)

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandIntegrationTest.java`

**Interfaces:**
- Consumes: `S3FileReaderCommandFactory` (Task 4), `S3FileReaderDto.Config` (Task 1); `TestUtils.buildJobContext(Map)`; LocalStack via Testcontainers (`localstack/localstack:3.5`, mirroring `S3BackendIntegrationTest`).

This task covers spec §9 cases 1, 2, 3, 4, 5, 6, 7. (Case 8 = Task 2; case 9 = Task 4.)

- [ ] **Step 1: Write the integration test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandIntegrationTest.java` (with license header):

```java
package io.fleak.zephflow.lib.commands.s3filereader;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Tag("integration")
@Testcontainers
class S3FileReaderCommandIntegrationTest {

  private static final String BUCKET = "test-bkt";
  private static final String CRED_ID = "s3-creds";

  @Container
  static LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5"))
          .withServices(LocalStackContainer.Service.S3)
          .withStartupTimeout(Duration.ofMinutes(2));

  private static String endpoint() {
    return LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString();
  }

  private static S3Client s3() {
    return S3Client.builder()
        .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .region(Region.of(LOCALSTACK.getRegion()))
        .forcePathStyle(true)
        .build();
  }

  @BeforeEach
  void setupBucket() {
    try (S3Client c = s3()) {
      c.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }
  }

  @AfterEach
  void teardownBucket() {
    try (S3Client c = s3()) {
      var objects = c.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).build());
      for (var obj : objects.contents()) {
        c.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(obj.key()).build());
      }
      c.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET).build());
    } catch (Exception ignored) {
    }
  }

  private void putText(String key, String body) {
    try (S3Client c = s3()) {
      c.putObject(
          PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
          RequestBody.fromString(body));
    }
  }

  private void putGzip(String key, String body) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(baos)) {
      gz.write(body.getBytes(StandardCharsets.UTF_8));
    }
    try (S3Client c = s3()) {
      c.putObject(
          PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
          RequestBody.fromBytes(baos.toByteArray()));
    }
  }

  private static JobContext jobContext() {
    Map<String, Serializable> props = new HashMap<>();
    props.put(
        CRED_ID,
        new UsernamePasswordCredential(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
    return TestUtils.buildJobContext(props);
  }

  private static Map<String, Object> baseConfig() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("pathField", "s3Path");
    cfg.put("region", LOCALSTACK.getRegion());
    cfg.put("credentialId", CRED_ID);
    cfg.put("s3EndpointOverride", endpoint());
    return cfg;
  }

  private ScalarCommand.ProcessResult run(Map<String, Object> configMap, String s3Path) {
    var cmd = new S3FileReaderCommandFactory().createCommand("node", jobContext());
    cmd.parseAndValidateArg(configMap);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var ctx = cmd.getExecutionContext();
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of("s3Path", s3Path));
    return ((ScalarCommand) cmd).process(List.of(input), "test_user", ctx);
  }

  @Test
  void plainTextLineMode() {
    putText("data/plain.log", "alpha\nbeta\ngamma");
    var result = run(baseConfig(), "s3://" + BUCKET + "/data/plain.log");
    assertTrue(result.getFailureEvents().isEmpty());
    List<RecordFleakData> out = result.getOutput();
    assertEquals(3, out.size());
    assertEquals("alpha", out.get(0).getPayload().get("line").unwrap());
    assertEquals("s3://" + BUCKET + "/data/plain.log", out.get(0).getPayload().get("file").unwrap());
  }

  @Test
  void gzipAutoMode() throws Exception {
    putGzip("data/app.log.gz", "one\ntwo\nthree\nfour");
    var result = run(baseConfig(), "s3://" + BUCKET + "/data/app.log.gz");
    assertTrue(result.getFailureEvents().isEmpty());
    assertEquals(4, result.getOutput().size());
    assertEquals("one", result.getOutput().get(0).getPayload().get("line").unwrap());
  }

  @Test
  void compressionNoneOverGzKeyDoesNotGunzip() throws Exception {
    putGzip("data/raw.log.gz", "hello\nworld");
    Map<String, Object> cfg = baseConfig();
    cfg.put("compression", "NONE");
    var result = run(cfg, "s3://" + BUCKET + "/data/raw.log.gz");
    // Reading gzip bytes as text (NONE) yields garbled content; never the clean source lines.
    boolean hasCleanFirstLine =
        result.getOutput().stream()
            .anyMatch(r -> "hello".equals(r.getPayload().get("line").unwrap()));
    assertFalse(hasCleanFirstLine, "NONE must not decompress the .gz object");
  }

  @Test
  void urlEncodedKeyResolves() {
    // Real object key contains a space; the record carries the '+'-encoded form.
    putText("data/foo bar.log", "x\ny");
    var result = run(baseConfig(), "s3://" + BUCKET + "/data/foo+bar.log");
    assertTrue(result.getFailureEvents().isEmpty());
    assertEquals(2, result.getOutput().size());
  }

  @Test
  void missingPathFieldProducesFailureEvent() {
    var cmd = new S3FileReaderCommandFactory().createCommand("node", jobContext());
    cmd.parseAndValidateArg(baseConfig());
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var ctx = cmd.getExecutionContext();
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of("other", "value"));
    var result = ((ScalarCommand) cmd).process(List.of(input), "test_user", ctx);
    assertTrue(result.getOutput().isEmpty());
    assertEquals(1, result.getFailureEvents().size());
  }

  @Test
  void missingObjectProducesFailureEvent() {
    var result = run(baseConfig(), "s3://" + BUCKET + "/data/does-not-exist.log");
    assertTrue(result.getOutput().isEmpty());
    assertEquals(1, result.getFailureEvents().size());
  }

  @Test
  void nonS3PathProducesFailureEvent() {
    var result = run(baseConfig(), "https://example.com/not-s3");
    assertTrue(result.getOutput().isEmpty());
    assertEquals(1, result.getFailureEvents().size());
  }

  @Test
  void deserializeModeJsonObjectLine() {
    putText("data/events.jsonl", "{\"a\":1}\n{\"a\":2}");
    Map<String, Object> cfg = baseConfig();
    cfg.put("emission", "DESERIALIZE");
    cfg.put("encodingType", "JSON_OBJECT_LINE");
    var result = run(cfg, "s3://" + BUCKET + "/data/events.jsonl");
    assertTrue(result.getFailureEvents().isEmpty());
    assertEquals(2, result.getOutput().size());
    assertEquals(1.0, ((Number) result.getOutput().get(0).getPayload().get("a").unwrap()).doubleValue());
  }
}
```

Note on the `plainTextLineMode` assertion: simplify the first-line check to
`assertEquals("alpha", out.get(0).getPayload().get("line").unwrap());` if the ternary reads awkward — both forms assert the same thing; prefer the simple form. (Left as a cleanup for the implementer; the simple form is correct.)

- [ ] **Step 2: Run the integration test**

Run: `./gradlew :lib:spotlessApply && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3filereader.S3FileReaderCommandIntegrationTest"`
Expected: PASS (all 8 tests). Requires Docker running for Testcontainers.

If `JSON_OBJECT_LINE` deserialization wraps numbers differently than `Number`, adjust the last assertion to compare against the deserializer's actual value type (inspect one record's payload) — the line count assertion is the load-bearing check.

- [ ] **Step 3: Commit**

```bash
git add lib/src/test/java/io/fleak/zephflow/lib/commands/s3filereader/S3FileReaderCommandIntegrationTest.java
git commit -m "test(s3filereader): add LocalStack integration tests"
```

---

### Task 6: Full build verification

**Files:** none (verification only).

- [ ] **Step 1: Run the full lib build**

Run: `./gradlew :lib:spotlessApply && ./gradlew :lib:compileJava :lib:test`
Expected: BUILD SUCCESSFUL, all `s3filereader` tests green, spotless clean.

- [ ] **Step 2: Confirm acceptance criteria (spec §10)**

Verify each box, citing the test that proves it:
- Registered + `INTERMEDIATE_COMMAND` → `S3FileReaderCommandUnitTest.registeredAsIntermediateCommand`.
- Download + auto-gunzip + `{line, file}` → `gzipAutoMode`, `plainTextLineMode`.
- URL-encoded keys with `urlDecodeKey=true` → `urlEncodedKeyResolves`.
- Credential lookup + clear error when set-but-missing → covered by `createExecutionContext` logic; the happy path is exercised by every integration test (all use `credentialId`).
- LocalStack `s3EndpointOverride` works → all integration tests.
- Per-event failures become failure events → `missingPathFieldProducesFailureEvent`, `missingObjectProducesFailureEvent`, `nonS3PathProducesFailureEvent`.
- All §9 tests pass; spotless clean; `:lib:compileJava :lib:test` green → Step 1.

- [ ] **Step 3 (optional manual smoke test, per spec §9):** With `ZEPHFLOW_LOCAL_DIR` wired into grid, run `sqssource -> eval -> s3filereader -> stdout` against the real `dev-private-vpc-flow-logs` bucket. This is a manual/out-of-band check, not part of the automated suite.

---

## Notes on spec decisions

- **`batchSize` (spec §6):** kept as a forward-compat config field (default 500) but **not wired to any behavior** — `processOneEvent` emits all lines as one `List`, matching the spec's "Reserved for future chunked emission … keep it for forward-compat." Call this out in the PR description.
- **S3 client construction reuses `S3Backend.client(S3BackendConfig)`** rather than re-implementing region/creds/endpoint handling — the credential resolution + set-but-missing error mirrors `FsSourceCommand.s3BackendConfig`.
- **Output field names `{line, file}`** match `LineEmissionStrategy` exactly (`Map.of("line", line, "file", file.key().urn())`).
