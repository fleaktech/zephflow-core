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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Manual, READ-ONLY end-to-end smoke test against REAL AWS. Disabled unless the required env vars
 * are set, so it never runs in CI. It does NOT write to S3: it runs the real source against an
 * existing queue, downloads the referenced objects read-only, and asserts records are emitted.
 *
 * <p>Run it explicitly, e.g.:
 *
 * <pre>
 *   export S3RT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
 *   export S3RT_REGION=us-east-1
 *   # credentials: rely on the default AWS chain (AWS_ACCESS_KEY_ID/.../profile/IAM role) or set:
 *   export S3RT_ACCESS_KEY=...               # optional
 *   export S3RT_SECRET_KEY=...               # optional
 *   export S3RT_ENCODING=STRING_LINE         # optional, default STRING_LINE (raw line per record)
 *   export S3RT_COMPRESSION=GZIP             # optional, gzip is auto-detected when unset
 *   export S3RT_MIN_RECORDS=1                # optional, records to wait for (default 1)
 *   export S3RT_TIMEOUT_SECONDS=180          # optional, how long to wait (default 180)
 *   ./gradlew :lib:test --tests \
 *     io.fleak.zephflow.lib.commands.s3realtimesource.S3RealtimeSourceRealAwsManualTest
 * </pre>
 *
 * <p>Prerequisites: the queue must already be receiving {@code s3:ObjectCreated:*} notifications
 * (e.g. from VPC flow logs) and have pending/incoming messages during the test window.
 *
 * <p>WARNING: the source DELETES every message it successfully processes from the queue. Point this
 * at a dedicated test queue, not one whose messages are consumed by something else.
 */
@Slf4j
@Tag("manual")
@EnabledIfEnvironmentVariable(named = "S3RT_QUEUE_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "S3RT_REGION", matches = ".+")
class S3RealtimeSourceRealAwsManualTest {

  private static final String QUEUE_URL = System.getenv("S3RT_QUEUE_URL");
  private static final String REGION = System.getenv("S3RT_REGION");
  private static final String ENCODING = envOrDefault("S3RT_ENCODING", "STRING_LINE");
  // Optional: force compression (e.g. GZIP). When unset, gzip is auto-detected from object bytes.
  private static final String COMPRESSION = System.getenv("S3RT_COMPRESSION");
  private static final String ACCESS_KEY = System.getenv("S3RT_ACCESS_KEY");
  private static final String SECRET_KEY = System.getenv("S3RT_SECRET_KEY");
  private static final String CRED_ID = "s3rt-cred";
  private static final int MIN_RECORDS = Integer.parseInt(envOrDefault("S3RT_MIN_RECORDS", "1"));
  private static final int TIMEOUT_SECONDS =
      Integer.parseInt(envOrDefault("S3RT_TIMEOUT_SECONDS", "180"));

  @Test
  void realAws_consumesRealNotifications() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    S3RealtimeSourceCommand command =
        new S3RealtimeSourceCommandFactory().createCommand("node", jobContext());
    command.parseAndValidateArg(config());
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    CollectingAcceptor acceptor = new CollectingAcceptor();
    try {
      executor.submit(
          () -> {
            try {
              command.execute("user", acceptor);
            } catch (Exception e) {
              log.error("source execution failed", e);
            }
          });

      log.info(
          "polling {} (encoding={}) for up to {}s, expecting >= {} record(s)",
          QUEUE_URL,
          ENCODING,
          TIMEOUT_SECONDS,
          MIN_RECORDS);
      waitUntil(() -> acceptor.records.size() >= MIN_RECORDS, Duration.ofSeconds(TIMEOUT_SECONDS));

      log.info(
          "emitted {} record(s); first record: {}",
          acceptor.records.size(),
          acceptor.records.isEmpty() ? "<none>" : acceptor.records.get(0).unwrap());
      assertTrue(
          acceptor.records.size() >= MIN_RECORDS,
          "expected at least "
              + MIN_RECORDS
              + " record(s) from real S3 notifications on the queue");
    } finally {
      command.terminate();
      executor.shutdownNow();
    }
  }

  private static Map<String, Object> config() {
    Map<String, Object> config = new HashMap<>();
    config.put("queueUrl", QUEUE_URL);
    config.put("regionStr", REGION);
    config.put("encodingType", ENCODING);
    if (COMPRESSION != null && !COMPRESSION.isBlank()) {
      config.put("compressionType", COMPRESSION);
    }
    config.put("waitTimeSeconds", 5);
    if (ACCESS_KEY != null && SECRET_KEY != null) {
      config.put("credentialId", CRED_ID);
    }
    return config;
  }

  private static JobContext jobContext() {
    Map<String, Serializable> otherProperties = new HashMap<>();
    if (ACCESS_KEY != null && SECRET_KEY != null) {
      otherProperties.put(
          CRED_ID, new HashMap<>(Map.of("username", ACCESS_KEY, "password", SECRET_KEY)));
    }
    return TestUtils.buildJobContext(otherProperties);
  }

  private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("condition not met within " + timeout);
  }

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static class CollectingAcceptor implements SourceEventAcceptor {
    final List<RecordFleakData> records = new CopyOnWriteArrayList<>();

    @Override
    public void accept(List<RecordFleakData> recordFleakData) {
      records.addAll(recordFleakData);
    }

    @Override
    public void terminate() {}
  }
}
