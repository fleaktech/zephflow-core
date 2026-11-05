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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.HashSet;
import java.util.List;
import java.util.SplittableRandom;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RawDataSamplerTest {

  private RawDataEncoder<SerializedEvent> mockEncoder;
  private DlqWriter mockWriter;
  private RawDataSampler<SerializedEvent> sampler;

  @BeforeEach
  void setUp() {
    mockEncoder = mock();
    mockWriter = mock(DlqWriter.class);
  }

  @AfterEach
  void tearDown() {
    if (sampler != null) {
      sampler.close();
    }
  }

  private RawDataSampler<SerializedEvent> sampler(long intervalMs, int perInterval) {
    return new RawDataSampler<>(
        mockEncoder, mockWriter, "node1", intervalMs, perInterval, new SplittableRandom(42));
  }

  private static List<SerializedEvent> events(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> new SerializedEvent(null, ("e" + i).getBytes(), null))
        .toList();
  }

  private List<SerializedEvent> written(int times) {
    ArgumentCaptor<SerializedEvent> captor = ArgumentCaptor.forClass(SerializedEvent.class);
    verify(mockEncoder, times(times)).serialize(captor.capture());
    verify(mockWriter, times(times)).writeToDlq(anyLong(), any(), eq(""), eq("node1"));
    return captor.getAllValues();
  }

  @Test
  void samplesOnFirstBatchBecauseBudgetStartsFull() {
    sampler = sampler(60_000, RawDataSampler.SAMPLES_PER_INTERVAL);
    sampler.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    when(mockEncoder.serialize(same(event))).thenReturn(event);

    sampler.maybeSample(List.of(event));

    verify(mockEncoder).serialize(same(event));
    verify(mockWriter).writeToDlq(anyLong(), same(event), eq(""), eq("node1"));
  }

  @Test
  void doesNotSampleBeyondTheBudgetBeforeTimerFires() {
    sampler = sampler(60_000, 1);
    sampler.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    when(mockEncoder.serialize(any())).thenReturn(event);

    sampler.maybeSample(List.of(event));
    sampler.maybeSample(List.of(event));

    verify(mockWriter, times(1)).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void largeBatchWritesTheBudgetFromDistinctRandomPositions() {
    sampler = sampler(60_000, RawDataSampler.SAMPLES_PER_INTERVAL);
    sampler.open();
    List<SerializedEvent> batch = events(50);
    when(mockEncoder.serialize(any())).thenAnswer(invocation -> invocation.getArgument(0));

    sampler.maybeSample(batch);

    List<SerializedEvent> written = written(RawDataSampler.SAMPLES_PER_INTERVAL);
    assertEquals(RawDataSampler.SAMPLES_PER_INTERVAL, new HashSet<>(written).size());
    assertNotEquals(batch.subList(0, RawDataSampler.SAMPLES_PER_INTERVAL), written);
  }

  @Test
  void batchesShareOneIntervalsBudget() {
    sampler = sampler(60_000, RawDataSampler.SAMPLES_PER_INTERVAL);
    sampler.open();
    when(mockEncoder.serialize(any())).thenAnswer(invocation -> invocation.getArgument(0));

    sampler.maybeSample(events(4));
    written(4);
    sampler.maybeSample(events(8));
    written(10);
    sampler.maybeSample(events(8));
    written(10);
  }

  @Test
  void timerReEnablesSampling() throws InterruptedException {
    sampler = sampler(200, 1);
    sampler.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    when(mockEncoder.serialize(any())).thenReturn(event);

    sampler.maybeSample(List.of(event));
    verify(mockWriter, times(1)).writeToDlq(anyLong(), any(), any(), any());

    Thread.sleep(500);

    sampler.maybeSample(List.of(event));
    verify(mockWriter, times(2)).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void skipsEmptyBatch() {
    sampler = sampler(60_000, RawDataSampler.SAMPLES_PER_INTERVAL);
    sampler.open();

    sampler.maybeSample(List.of());

    verifyNoInteractions(mockEncoder);
    verify(mockWriter, never()).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void encoderErrorDoesNotPropagate() {
    sampler = sampler(60_000, RawDataSampler.SAMPLES_PER_INTERVAL);
    sampler.open();

    List<SerializedEvent> batch = events(2);
    when(mockEncoder.serialize(same(batch.get(0))))
        .thenThrow(new RuntimeException("encode failed"));
    when(mockEncoder.serialize(same(batch.get(1)))).thenReturn(batch.get(1));

    sampler.maybeSample(batch);

    verify(mockWriter).writeToDlq(anyLong(), same(batch.get(1)), eq(""), eq("node1"));
  }

  @Test
  void noOpSamplerDoesNothing() {
    RawDataSampler<SerializedEvent> noOp = RawDataSampler.noOp();
    noOp.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    noOp.maybeSample(List.of(event));
    noOp.close();
    // no exceptions, no interactions
  }
}
