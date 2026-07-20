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
          PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromString(body));
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
    assertEquals(
        "s3://" + BUCKET + "/data/plain.log", out.get(0).getPayload().get("file").unwrap());
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
    assertEquals(
        1.0, ((Number) result.getOutput().get(0).getPayload().get("a").unwrap()).doubleValue());
  }
}
