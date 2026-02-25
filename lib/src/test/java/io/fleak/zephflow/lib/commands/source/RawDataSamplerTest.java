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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @Test
  void samplesOnFirstBatchBecauseFlagStartsTrue() {
    sampler = new RawDataSampler<>(mockEncoder, mockWriter, "node1", 60_000);
    sampler.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    when(mockEncoder.serialize(same(event))).thenReturn(event);

    sampler.maybeSample(List.of(event));

    verify(mockEncoder).serialize(same(event));
    verify(mockWriter).writeToDlq(anyLong(), same(event), eq(""), eq("node1"));
  }

  @Test
  void doesNotSampleSecondBatchBeforeTimerFires() {
    sampler = new RawDataSampler<>(mockEncoder, mockWriter, "node1", 60_000);
    sampler.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    when(mockEncoder.serialize(any())).thenReturn(event);

    sampler.maybeSample(List.of(event));
    sampler.maybeSample(List.of(event));

    verify(mockWriter, times(1)).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void timerReEnablesSampling() throws InterruptedException {
    sampler = new RawDataSampler<>(mockEncoder, mockWriter, "node1", 200);
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
    sampler = new RawDataSampler<>(mockEncoder, mockWriter, "node1", 60_000);
    sampler.open();

    sampler.maybeSample(List.of());

    verifyNoInteractions(mockEncoder);
    verify(mockWriter, never()).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void encoderErrorDoesNotPropagate() {
    sampler = new RawDataSampler<>(mockEncoder, mockWriter, "node1", 60_000);
    sampler.open();

    SerializedEvent event = new SerializedEvent(null, "data".getBytes(), null);
    when(mockEncoder.serialize(any())).thenThrow(new RuntimeException("encode failed"));

    sampler.maybeSample(List.of(event));
    // no exception thrown
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
