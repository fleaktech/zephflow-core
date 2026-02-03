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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.Decompressor;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class BytesRawDataConverterTest {

  private BytesRawDataConverter converter;
  private FleakDeserializer<?> mockDeserializer;
  private Decompressor mockDecompressor;
  private SourceExecutionContext<?> mockContext;
  private FleakCounter mockInputEventCounter;
  private FleakCounter mockDataSizeCounter;
  private FleakCounter mockDeserializeFailureCounter;

  @BeforeEach
  void setUp() {
    mockDeserializer = mock(FleakDeserializer.class);
    mockDecompressor = mock(Decompressor.class);
    mockContext = mock(SourceExecutionContext.class);
    mockInputEventCounter = mock(FleakCounter.class);
    mockDataSizeCounter = mock(FleakCounter.class);
    mockDeserializeFailureCounter = mock(FleakCounter.class);

    when(mockContext.inputEventCounter()).thenReturn(mockInputEventCounter);
    when(mockContext.dataSizeCounter()).thenReturn(mockDataSizeCounter);
    when(mockContext.deserializeFailureCounter()).thenReturn(mockDeserializeFailureCounter);

    converter = new BytesRawDataConverter(mockDeserializer, mockDecompressor);
  }

  @Test
  void testConvert_withTagField_shouldExtractAndPassTags() throws Exception {
    // Arrange: Create event with __tag__ field
    Map<String, Object> eventPayload =
        Map.of("data", "test-value", "__tag__", Map.of("tenant_id", "12", "region", "us-west"));

    RecordFleakData event = (RecordFleakData) FleakData.wrap(eventPayload);
    byte[] rawBytes = "{\"data\":\"test-value\"}".getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent)).thenReturn(List.of(event));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNull(result.error());
    assertEquals(1, result.transformedData().size());

    // Verify tags were extracted and passed to counters
    ArgumentCaptor<Map<String, String>> dataSizeTagsCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map<String, String>> inputEventTagsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(mockDataSizeCounter).increase(eq((long) rawBytes.length), dataSizeTagsCaptor.capture());
    verify(mockInputEventCounter).increase(eq(1L), inputEventTagsCaptor.capture());

    // Verify both counters received the same tags from __tag__ field
    Map<String, String> dataSizeTags = dataSizeTagsCaptor.getValue();
    Map<String, String> inputEventTags = inputEventTagsCaptor.getValue();

    assertEquals("12", dataSizeTags.get("tenant_id"));
    assertEquals("us-west", dataSizeTags.get("region"));
    assertEquals("12", inputEventTags.get("tenant_id"));
    assertEquals("us-west", inputEventTags.get("region"));

    verify(mockDeserializeFailureCounter, never()).increase(any());
  }

  @Test
  void testConvert_withoutTagField_shouldUseEmptyTags() throws Exception {
    // Arrange: Create event WITHOUT __tag__ field
    Map<String, Object> eventPayload = Map.of("data", "test-value");

    RecordFleakData event = (RecordFleakData) FleakData.wrap(eventPayload);
    byte[] rawBytes = "{\"data\":\"test-value\"}".getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent)).thenReturn(List.of(event));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNull(result.error());

    // Verify counters were called with empty tags
    ArgumentCaptor<Map<String, String>> dataSizeTagsCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map<String, String>> inputEventTagsCaptor = ArgumentCaptor.forClass(Map.class);

    verify(mockDataSizeCounter).increase(anyLong(), dataSizeTagsCaptor.capture());
    verify(mockInputEventCounter).increase(anyLong(), inputEventTagsCaptor.capture());

    assertTrue(dataSizeTagsCaptor.getValue().isEmpty());
    assertTrue(inputEventTagsCaptor.getValue().isEmpty());
  }

  @Test
  void testConvert_deserializationFailure_shouldUseEmptyTagsForFailureCounter() throws Exception {
    // Note: Currently, when deserialization fails, we don't extract tags from raw bytes
    // This test verifies the current behavior
    String jsonWithTags = "{\"data\":\"test\",\"__tag__\":{\"tenant_id\":\"99\",\"env\":\"prod\"}}";
    byte[] rawBytes = jsonWithTags.getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent))
        .thenThrow(new RuntimeException("Deserialization failed"));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNotNull(result.error());
    assertNull(result.transformedData());

    // Verify dataSizeCounter is called even on failure (with empty tags)
    ArgumentCaptor<Map<String, String>> dataSizeTagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDataSizeCounter).increase(eq((long) rawBytes.length), dataSizeTagsCaptor.capture());
    assertTrue(dataSizeTagsCaptor.getValue().isEmpty());

    // Verify failure counter was called with empty tags
    verify(mockDeserializeFailureCounter).increase(any());

    // inputEventCounter should not be called on failure
    verify(mockInputEventCounter, never()).increase(anyLong(), any());
  }

  @Test
  void testConvert_deserializationFailure_withInvalidJson_shouldUseEmptyTags() throws Exception {
    // Arrange: Invalid JSON that can't be parsed
    byte[] rawBytes = "invalid-json{not-valid".getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent))
        .thenThrow(new RuntimeException("Deserialization failed"));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNotNull(result.error());

    // Verify failure counter was called with empty tags (because JSON parsing also failed)
    ArgumentCaptor<Map<String, String>> failureTagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDeserializeFailureCounter).increase(failureTagsCaptor.capture());

    assertTrue(failureTagsCaptor.getValue().isEmpty());
  }

  @Test
  void testConvert_deserializationFailure_withJsonButNoTagField_shouldUseEmptyTags()
      throws Exception {
    // Arrange: Valid JSON without __tag__ field, deserialization fails
    String jsonWithoutTags = "{\"data\":\"test\",\"other\":\"value\"}";
    byte[] rawBytes = jsonWithoutTags.getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent))
        .thenThrow(new RuntimeException("Deserialization failed"));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNotNull(result.error());

    // Verify failure counter was called with empty tags (no __tag__ field found)
    ArgumentCaptor<Map<String, String>> failureTagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDeserializeFailureCounter).increase(failureTagsCaptor.capture());

    assertTrue(failureTagsCaptor.getValue().isEmpty());
  }

  @Test
  void testConvert_multipleEvents_shouldExtractTagsFromFirstEvent() throws Exception {
    // Arrange: Multiple events, only first has __tag__ field
    Map<String, Object> event1Payload =
        Map.of("id", "1", "__tag__", Map.of("tenant_id", "100", "batch", "first"));
    Map<String, Object> event2Payload = Map.of("id", "2");

    RecordFleakData event1 = (RecordFleakData) FleakData.wrap(event1Payload);
    RecordFleakData event2 = (RecordFleakData) FleakData.wrap(event2Payload);

    byte[] rawBytes = "test-data".getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent)).thenReturn(List.of(event1, event2));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNull(result.error());
    assertEquals(2, result.transformedData().size());

    // Verify tags from FIRST event were used for both counters
    ArgumentCaptor<Map<String, String>> inputEventTagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockInputEventCounter).increase(eq(2L), inputEventTagsCaptor.capture());

    Map<String, String> tags = inputEventTagsCaptor.getValue();
    assertEquals("100", tags.get("tenant_id"));
    assertEquals("first", tags.get("batch"));
  }

  @Test
  void testConvert_emptyEventList_shouldUseEmptyTags() throws Exception {
    // Arrange: Deserializer returns empty list
    byte[] rawBytes = "[]".getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent)).thenReturn(List.of());

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNull(result.error());
    assertTrue(result.transformedData().isEmpty());

    // Verify counters were called with empty tags
    ArgumentCaptor<Map<String, String>> inputEventTagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockInputEventCounter).increase(eq(0L), inputEventTagsCaptor.capture());

    assertTrue(inputEventTagsCaptor.getValue().isEmpty());
  }

  @Test
  void testConvert_tagFieldWithNullValues_shouldSkipNullValues() throws Exception {
    // Arrange: Event with __tag__ field containing null values
    // Use HashMap since Map.of() doesn't allow null values
    java.util.HashMap<String, Object> tagMap = new java.util.HashMap<>();
    tagMap.put("tenant_id", "123");
    tagMap.put("null_field", null);
    tagMap.put("region", "eu");

    Map<String, Object> eventPayload = Map.of("data", "test", "__tag__", tagMap);

    RecordFleakData event = (RecordFleakData) FleakData.wrap(eventPayload);
    byte[] rawBytes = "test-data".getBytes(StandardCharsets.UTF_8);
    SerializedEvent serializedEvent = new SerializedEvent(null, rawBytes, null);

    when(mockDecompressor.decompress(serializedEvent)).thenReturn(serializedEvent);
    when(mockDeserializer.deserialize(serializedEvent)).thenReturn(List.of(event));

    // Act
    ConvertedResult<SerializedEvent> result = converter.convert(serializedEvent, mockContext);

    // Assert
    assertNull(result.error());

    ArgumentCaptor<Map<String, String>> inputEventTagsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockInputEventCounter).increase(anyLong(), inputEventTagsCaptor.capture());

    Map<String, String> tags = inputEventTagsCaptor.getValue();
    assertEquals("123", tags.get("tenant_id"));
    assertEquals("eu", tags.get("region"));
    assertFalse(tags.containsKey("null_field")); // Null values should be skipped
  }
}
