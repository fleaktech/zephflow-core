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
package io.fleak.zephflow.lib.commands.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3FlusherTest {

  private final String bucketName = "test-bucket";
  private final String keyName = "test-key";
  private S3Client s3Client;
  private S3Flusher s3Flusher;
  private List<RecordFleakData> events;
  private SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents;

  @BeforeEach
  void setUp() {
    s3Client = mock(S3Client.class);

    events =
        List.of(
            (RecordFleakData)
                Objects.requireNonNull(FleakData.wrap(Map.of("name", "Alice", "age", 30))),
            (RecordFleakData)
                Objects.requireNonNull(FleakData.wrap(Map.of("name", "Bob", "city", "New, York"))));

    preparedInputEvents = new SimpleSinkCommand.PreparedInputEvents<>();
    events.forEach(e -> preparedInputEvents.add(null, e));
  }

  @Test
  void testFlush_AsJson() throws Exception {

    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_ARRAY).createSerializer();

    S3Commiter<RecordFleakData> commiter =
        new OnDemandS3Commiter(s3Client, bucketName, keyName, serializer);
    s3Flusher = new S3Flusher(commiter);

    SimpleSinkCommand.FlushResult result = s3Flusher.flush(preparedInputEvents, Map.of());

    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

    verify(s3Client, times(1))
        .putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    PutObjectRequest capturedRequest = putObjectRequestCaptor.getValue();
    assertEquals(bucketName, capturedRequest.bucket());
    assertTrue(capturedRequest.key().startsWith(keyName));
    assertTrue(capturedRequest.key().endsWith(".json"));

    String capturedContent =
        new BufferedReader(
                new InputStreamReader(
                    requestBodyCaptor.getValue().contentStreamProvider().newStream(),
                    StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    ArrayFleakData arrayFleakData = new ArrayFleakData();
    arrayFleakData.getArrayPayload().addAll(events);
    assertEquals(
        JsonUtils.OBJECT_MAPPER.readTree(JsonUtils.toArrayNode(arrayFleakData).toString()),
        JsonUtils.OBJECT_MAPPER.readTree(capturedContent));

    assertEquals(events.size(), result.successCount());
  }

  @Test
  void testFlush_AsJsonl() throws Exception {
    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT_LINE).createSerializer();
    S3Commiter<RecordFleakData> commiter =
        new OnDemandS3Commiter(s3Client, bucketName, keyName, serializer);
    s3Flusher = new S3Flusher(commiter);

    SimpleSinkCommand.FlushResult result = s3Flusher.flush(preparedInputEvents, Map.of());

    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

    verify(s3Client, times(1))
        .putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    PutObjectRequest capturedRequest = putObjectRequestCaptor.getValue();
    assertEquals(bucketName, capturedRequest.bucket());
    assertTrue(capturedRequest.key().startsWith(keyName));
    assertTrue(capturedRequest.key().endsWith(".jsonl"));

    String capturedContent =
        new BufferedReader(
                new InputStreamReader(
                    requestBodyCaptor.getValue().contentStreamProvider().newStream(),
                    StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    String[] capturedLines = capturedContent.split("\n");
    assertEquals(2, capturedLines.length);
    assertEquals(
        JsonUtils.OBJECT_MAPPER.readTree("{\"name\":\"Alice\",\"age\":30}"),
        JsonUtils.OBJECT_MAPPER.readTree(capturedLines[0]));
    assertEquals(
        JsonUtils.OBJECT_MAPPER.readTree("{\"name\":\"Bob\",\"city\":\"New, York\"}"),
        JsonUtils.OBJECT_MAPPER.readTree(capturedLines[1]));

    assertEquals(events.size(), result.successCount());
  }

  @Test
  void testFlush_AsCsv() throws Exception {

    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(EncodingType.CSV).createSerializer();
    S3Commiter<RecordFleakData> commiter =
        new OnDemandS3Commiter(s3Client, bucketName, keyName, serializer);
    s3Flusher = new S3Flusher(commiter);

    SimpleSinkCommand.FlushResult result = s3Flusher.flush(preparedInputEvents, Map.of());

    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor =
        ArgumentCaptor.forClass(PutObjectRequest.class);
    ArgumentCaptor<RequestBody> requestBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

    verify(s3Client, times(1))
        .putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    PutObjectRequest capturedRequest = putObjectRequestCaptor.getValue();
    assertEquals(bucketName, capturedRequest.bucket());
    assertTrue(capturedRequest.key().startsWith(keyName));
    assertTrue(capturedRequest.key().endsWith(".csv"));

    String capturedContent =
        new BufferedReader(
                new InputStreamReader(
                    requestBodyCaptor.getValue().contentStreamProvider().newStream(),
                    StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    String expectedContent =
        """
        name,age,city
        Alice,30,
        Bob,,"New, York\"""";
    assertEquals(expectedContent, capturedContent);

    assertEquals(events.size(), result.successCount());
  }
}
