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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class S3RealtimeRawDataConverterTest {

  private static final long MAX_OBJECT_SIZE = 1024 * 1024;

  private S3Client s3Client;
  private Queue<String> confirmed;
  private DlqWriter dlqWriter;
  private FleakCounter skippedObjectCounter;
  private SourceExecutionContext<S3EventMessage> ctx;

  @BeforeEach
  void setUp() {
    s3Client = mock(S3Client.class);
    confirmed = new ConcurrentLinkedQueue<>();
    dlqWriter = mock(DlqWriter.class);
    skippedObjectCounter = mock(FleakCounter.class);
    ctx =
        new SourceExecutionContext<>(
            null,
            null,
            null,
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            null);
  }

  private S3RealtimeRawDataConverter converter(EncodingType encodingType, boolean addS3Metadata) {
    return converter(encodingType, addS3Metadata, null);
  }

  private S3RealtimeRawDataConverter converter(
      EncodingType encodingType, boolean addS3Metadata, CompressionType compressionType) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();
    return new S3RealtimeRawDataConverter(
        s3Client,
        deserializer,
        compressionType,
        MAX_OBJECT_SIZE,
        addS3Metadata,
        confirmed,
        dlqWriter,
        new S3RealtimeRawDataEncoder(),
        "node",
        skippedObjectCounter);
  }

  private void stubGetObject(String bucket, String key, byte[] data) {
    ResponseInputStream<GetObjectResponse> stream =
        new ResponseInputStream<>(
            GetObjectResponse.builder().contentLength((long) data.length).build(),
            AbortableInputStream.create(new ByteArrayInputStream(data)));
    when(s3Client.getObject(argThatMatches(bucket, key))).thenReturn(stream);
  }

  private static GetObjectRequest argThatMatches(String bucket, String key) {
    return org.mockito.ArgumentMatchers.argThat(
        (GetObjectRequest r) -> r != null && bucket.equals(r.bucket()) && key.equals(r.key()));
  }

  @Test
  void convert_jsonObjectLineSuccess() {
    byte[] body = "{\"msg\":\"a\"}\n{\"msg\":\"b\"}".getBytes(StandardCharsets.UTF_8);
    stubGetObject("b", "k.jsonl", body);
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", null, List.of(new S3ObjectRef("b", "k.jsonl")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    List<RecordFleakData> expected =
        List.of(record(Map.of("msg", "a")), record(Map.of("msg", "b")));
    assertEquals(expected, result.transformedData());
    assertNull(result.error());
    assertEquals(List.of("r1"), List.copyOf(confirmed));
  }

  @Test
  void convert_addsS3Metadata() {
    byte[] body = "{\"msg\":\"a\"}".getBytes(StandardCharsets.UTF_8);
    stubGetObject("my-bucket", "dir/k.json", body);
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", null, List.of(new S3ObjectRef("my-bucket", "dir/k.json")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, true).convert(msg, ctx);

    Map<String, Object> expectedPayload = new LinkedHashMap<>();
    expectedPayload.put("msg", "a");
    expectedPayload.put("__s3_bucket", "my-bucket");
    expectedPayload.put("__s3_key", "dir/k.json");
    assertEquals(List.of(record(expectedPayload)), result.transformedData());
  }

  @Test
  void convert_multipleRefsDownloadedOneAtATime() {
    stubGetObject("b", "k1.jsonl", "{\"msg\":\"a\"}".getBytes(StandardCharsets.UTF_8));
    stubGetObject("b", "k2.jsonl", "{\"msg\":\"b\"}".getBytes(StandardCharsets.UTF_8));
    S3EventMessage msg =
        new S3EventMessage(
            "m1",
            "r1",
            null,
            List.of(new S3ObjectRef("b", "k1.jsonl"), new S3ObjectRef("b", "k2.jsonl")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    assertEquals(
        List.of(record(Map.of("msg", "a")), record(Map.of("msg", "b"))), result.transformedData());
    verify(s3Client, times(2)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void convert_oversizedObjectDlqdAndAcknowledged() {
    ResponseInputStream<GetObjectResponse> stream =
        new ResponseInputStream<>(
            GetObjectResponse.builder().contentLength(MAX_OBJECT_SIZE + 1).build(),
            AbortableInputStream.create(new ByteArrayInputStream(new byte[0])));
    when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(stream);
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", "raw-body", List.of(new S3ObjectRef("b", "big.json")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    // Oversized is a real drop (the object exists): dead-lettered with the reason + acknowledged,
    // not silently skipped.
    assertNull(result.error());
    assertEquals(List.of(), result.transformedData());
    assertEquals(List.of("r1"), List.copyOf(confirmed));
    verify(dlqWriter)
        .writeToDlq(anyLong(), any(), contains("exceeds maxObjectSizeBytes"), eq("node"));
    verify(skippedObjectCounter).increase(Map.of("reason", "oversized"));
  }

  @Test
  void convert_missingObjectSkippedAndAcknowledged() {
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenThrow(
            software.amazon.awssdk.services.s3.model.NoSuchKeyException.builder()
                .message("The specified key does not exist.")
                .build());
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", null, List.of(new S3ObjectRef("b", "gone.jsonl")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    // A deleted object is a benign skip: acknowledged (no records), no DLQ, but counted.
    assertNull(result.error());
    assertEquals(List.of(), result.transformedData());
    assertEquals(List.of("r1"), List.copyOf(confirmed));
    verifyNoInteractions(dlqWriter);
    verify(skippedObjectCounter).increase(Map.of("reason", "missing"));
  }

  @Test
  void convert_deserializeFailureDlqdAndAcknowledged() {
    stubGetObject("b", "bad.jsonl", "not valid json".getBytes(StandardCharsets.UTF_8));
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", "raw-body", List.of(new S3ObjectRef("b", "bad.jsonl")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    // Terminal: dead-lettered with the real error + acknowledged, not a retryable failure.
    assertNull(result.error());
    assertEquals(List.of(), result.transformedData());
    assertEquals(List.of("r1"), List.copyOf(confirmed));
    verify(dlqWriter)
        .writeToDlq(anyLong(), any(), contains("failed to process s3://b/bad.jsonl"), eq("node"));
  }

  @Test
  void convert_downloadFailureDlqdAndAcknowledged() {
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenThrow(new RuntimeException("s3 unavailable"));
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", "raw-body", List.of(new S3ObjectRef("b", "k.json")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    assertNull(result.error());
    assertEquals(List.of(), result.transformedData());
    assertEquals(List.of("r1"), List.copyOf(confirmed));
    verify(dlqWriter).writeToDlq(anyLong(), any(), contains("s3 unavailable"), eq("node"));
  }

  @Test
  void convert_multiObjectPartialFailureEmitsGoodAndDlqsBad() {
    stubGetObject("b", "good.jsonl", "{\"msg\":\"a\"}".getBytes(StandardCharsets.UTF_8));
    stubGetObject("b", "bad.jsonl", "not valid json".getBytes(StandardCharsets.UTF_8));
    S3EventMessage msg =
        new S3EventMessage(
            "m1",
            "r1",
            "raw-body",
            List.of(new S3ObjectRef("b", "good.jsonl"), new S3ObjectRef("b", "bad.jsonl")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false).convert(msg, ctx);

    // Good object emitted, bad object dead-lettered, message acknowledged.
    assertEquals(List.of(record(Map.of("msg", "a"))), result.transformedData());
    assertEquals(List.of("r1"), List.copyOf(confirmed));
    verify(dlqWriter)
        .writeToDlq(anyLong(), any(), contains("failed to process s3://b/bad.jsonl"), eq("node"));
  }

  @Test
  void convert_autoDetectsGzip() {
    stubGetObject("b", "data.jsonl.gz", gzip("{\"msg\":\"a\"}\n{\"msg\":\"b\"}"));
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", null, List.of(new S3ObjectRef("b", "data.jsonl.gz")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false, null).convert(msg, ctx);

    assertEquals(
        List.of(record(Map.of("msg", "a")), record(Map.of("msg", "b"))), result.transformedData());
    assertNull(result.error());
  }

  @Test
  void convert_explicitGzip() {
    stubGetObject("b", "data", gzip("{\"msg\":\"a\"}"));
    S3EventMessage msg =
        new S3EventMessage("m1", "r1", null, List.of(new S3ObjectRef("b", "data")));

    ConvertedResult<S3EventMessage> result =
        converter(EncodingType.JSON_OBJECT_LINE, false, CompressionType.GZIP).convert(msg, ctx);

    assertEquals(List.of(record(Map.of("msg", "a"))), result.transformedData());
  }

  private static RecordFleakData record(Map<String, Object> payload) {
    return (RecordFleakData) FleakData.wrap(payload);
  }

  private static byte[] gzip(String content) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (GZIPOutputStream gos = new GZIPOutputStream(bos)) {
      gos.write(content.getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return bos.toByteArray();
  }
}
