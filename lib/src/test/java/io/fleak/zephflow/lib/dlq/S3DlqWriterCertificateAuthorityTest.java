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
package io.fleak.zephflow.lib.dlq;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_DLQ_RECORDS_NOT_WRITTEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The dead letter writer of a store whose certificate a private authority issued: it trusts the
 * authorities of its config besides the JVM's default ones, and nothing else in the JVM does.
 */
@Timeout(60)
class S3DlqWriterCertificateAuthorityTest {

  private static final String REGION = "us-east-1";
  private static final String BUCKET = "dead-letters";
  private static final String KEY_PREFIX = "dev/pipeline-1/deployment-1/run-1";
  private static final TestCertificateAuthority STORE_AUTHORITY =
      TestCertificateAuthority.create("Store Authority");
  private static final TestCertificateAuthority OTHER_AUTHORITY =
      TestCertificateAuthority.create("Other Authority");

  private final String accessKey = "access-" + UUID.randomUUID();
  private final String secretKey = "x-" + UUID.randomUUID();
  private final MetricClientProvider metricClientProvider = mock(MetricClientProvider.class);
  private final FleakCounter unwrittenDeadLetterCounter = mock(FleakCounter.class);
  private HttpsObjectStore store;

  record StoredDeadLetter(String value, String errorMessage, String nodeId) {}

  @BeforeEach
  void startStore() throws Exception {
    store = new HttpsObjectStore(STORE_AUTHORITY.serverContext());
    when(metricClientProvider.counter(METRIC_NAME_DLQ_RECORDS_NOT_WRITTEN, Map.of()))
        .thenReturn(unwrittenDeadLetterCounter);
  }

  @AfterEach
  void stopStore() {
    store.close();
  }

  @Test
  void writesDeadLettersToAStoreWhoseCertificateTheConfiguredAuthorityIssued() throws Exception {
    try (DlqWriter writer =
        DlqWriterFactory.createDlqWriter(
            config(STORE_AUTHORITY.certificatePem()), KEY_PREFIX, metricClientProvider, Map.of())) {
      writer.open();
      writeTwoDeadLetters(writer);
    }

    Map<String, byte[]> objects = store.objects();
    assertEquals(1, objects.size(), objects.keySet().toString());
    String path = objects.keySet().iterator().next();
    assertTrue(
        path.matches(
            "/" + BUCKET + "/" + KEY_PREFIX + "/dead-letters/dt=.+/hr=\\d{2}/.+_n2\\.avro"),
        path);
    assertEquals(
        List.of(
            new StoredDeadLetter("value-0", "error-0", "node-1"),
            new StoredDeadLetter("value-1", "error-1", "node-1")),
        readDeadLetters(objects.get(path)));
    verifyNoInteractions(unwrittenDeadLetterCounter);
  }

  @Test
  void writesRawDataSamplesToTheStoreWithTheConfiguredAuthority() throws Exception {
    try (DlqWriter writer =
        DlqWriterFactory.createSampleWriter(config(STORE_AUTHORITY.certificatePem()), KEY_PREFIX)) {
      writer.open();
      writeTwoDeadLetters(writer);
    }

    List<String> paths = List.copyOf(store.objects().keySet());
    assertEquals(1, paths.size(), paths.toString());
    assertTrue(
        paths
            .getFirst()
            .matches("/" + BUCKET + "/" + KEY_PREFIX + "/raw-data-samples/.+_n2\\.avro"),
        paths.getFirst());
  }

  static Stream<Arguments> untrustedAuthorities() {
    return Stream.of(
        Arguments.of("default trust only", null),
        Arguments.of("another authority", OTHER_AUTHORITY.certificatePem()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("untrustedAuthorities")
  void countsDeadLettersAsNotWrittenWhenTheStoresAuthorityIsNotTrusted(
      String trust, String caCertificates) throws Exception {
    try (DlqWriter writer =
        DlqWriterFactory.createDlqWriter(
            config(caCertificates), KEY_PREFIX, metricClientProvider, Map.of())) {
      writer.open();
      writeTwoDeadLetters(writer);
    }

    assertEquals(Map.of(), store.objects());
    verify(unwrittenDeadLetterCounter).increase(2, Map.of());
  }

  @Test
  void otherClientsInTheJvmKeepTheDefaultTrust() throws Exception {
    try (DlqWriter writer =
        DlqWriterFactory.createDlqWriter(
            config(STORE_AUTHORITY.certificatePem()), KEY_PREFIX, metricClientProvider, Map.of())) {
      writer.open();
      writeTwoDeadLetters(writer);
    }
    assertEquals(1, store.objects().size());

    try (S3Client plainClient =
        new AwsClientFactory()
            .createS3Client(
                REGION, new UsernamePasswordCredential(accessKey, secretKey), store.endpoint())) {
      SdkClientException refused =
          assertThrows(
              SdkClientException.class,
              () ->
                  plainClient.putObject(
                      request -> request.bucket(BUCKET).key("plain"),
                      RequestBody.fromString("plain")));
      assertTrue(
          ExceptionUtils.indexOfType(refused, SSLHandshakeException.class) >= 0,
          ExceptionUtils.getStackTrace(refused));
    }
    try (HttpClient httpClient = HttpClient.newHttpClient()) {
      assertThrows(
          SSLHandshakeException.class,
          () ->
              httpClient.send(
                  HttpRequest.newBuilder(URI.create(store.endpoint())).build(),
                  HttpResponse.BodyHandlers.discarding()));
    }
  }

  @Test
  void factoryClientWithAdditionalAuthoritiesReachesTheStore() {
    try (S3Client client =
        new AwsClientFactory()
            .createS3Client(
                REGION,
                new UsernamePasswordCredential(accessKey, secretKey),
                store.endpoint(),
                STORE_AUTHORITY.certificatePem())) {
      client.putObject(
          request -> request.bucket(BUCKET).key("object"), RequestBody.fromString("content"));
    }

    assertEquals(List.of("/" + BUCKET + "/object"), List.copyOf(store.objects().keySet()));
  }

  @Test
  void refusesCertificateAuthoritiesThatAreNotPemCertificates() {
    IllegalArgumentException refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DlqWriterFactory.createDlqWriter(
                    config("not a certificate"), KEY_PREFIX, metricClientProvider, Map.of()));
    assertEquals(
        "The additional CA certificates are not readable PEM certificates", refused.getMessage());
  }

  private JobContext.S3DlqConfig config(String caCertificates) {
    return JobContext.S3DlqConfig.builder()
        .region(REGION)
        .bucket(BUCKET)
        .batchSize(100)
        .flushIntervalMillis(60_000)
        .accessKeyId(accessKey)
        .secretAccessKey(secretKey)
        .s3EndpointOverride(store.endpoint())
        .caCertificates(caCertificates)
        .build();
  }

  private static void writeTwoDeadLetters(DlqWriter writer) {
    for (int i = 0; i < 2; i++) {
      writer.writeToDlq(
          1_700_000_000_000L + i,
          new SerializedEvent(
              ("key-" + i).getBytes(StandardCharsets.UTF_8),
              ("value-" + i).getBytes(StandardCharsets.UTF_8),
              Map.of("header", "h")),
          "error-" + i,
          "node-1");
    }
  }

  private static List<StoredDeadLetter> readDeadLetters(byte[] object) throws Exception {
    List<StoredDeadLetter> deadLetters = new ArrayList<>();
    try (DataFileReader<DeadLetter> reader =
        new DataFileReader<>(
            new SeekableByteArrayInput(object), new SpecificDatumReader<>(DeadLetter.class))) {
      while (reader.hasNext()) {
        DeadLetter deadLetter = reader.next();
        deadLetters.add(
            new StoredDeadLetter(
                utf8(deadLetter.getValue()),
                deadLetter.getErrorMessage().toString(),
                deadLetter.getNodeId().toString()));
      }
    }
    return deadLetters;
  }

  private static String utf8(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
