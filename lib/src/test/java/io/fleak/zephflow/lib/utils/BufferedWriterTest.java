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
package io.fleak.zephflow.lib.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BufferedWriterTest {

  @Test
  void testSyncModeFlushesImmediately() {
    List<List<String>> flushedBatches = new ArrayList<>();

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(100, 1000, flushedBatches::add, "test-thread", true)) {
      writer.start();

      writer.write("item1");
      assertEquals(1, flushedBatches.size());
      assertEquals(List.of("item1"), flushedBatches.get(0));

      writer.write(List.of("item2", "item3"));
      assertEquals(2, flushedBatches.size());
      assertEquals(List.of("item2", "item3"), flushedBatches.get(1));
    }
  }

  @Test
  void testBatchSizeTriggersFlush() {
    List<List<String>> flushedBatches = new ArrayList<>();

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(3, 100_000, flushedBatches::add, "test-thread", false)) {
      writer.start();

      writer.write("item1");
      assertEquals(0, flushedBatches.size());
      assertEquals(1, writer.getBufferSize());

      writer.write("item2");
      assertEquals(0, flushedBatches.size());
      assertEquals(2, writer.getBufferSize());

      writer.write("item3");
      assertEquals(1, flushedBatches.size());
      assertEquals(List.of("item1", "item2", "item3"), flushedBatches.get(0));
      assertEquals(0, writer.getBufferSize());
    }
  }

  @Test
  void testManualFlush() {
    List<List<String>> flushedBatches = new ArrayList<>();

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(100, 100_000, flushedBatches::add, "test-thread", false)) {
      writer.start();

      writer.write("item1");
      writer.write("item2");
      assertEquals(0, flushedBatches.size());
      assertEquals(2, writer.getBufferSize());

      writer.flush();
      assertEquals(1, flushedBatches.size());
      assertEquals(List.of("item1", "item2"), flushedBatches.get(0));
      assertEquals(0, writer.getBufferSize());
    }
  }

  @Test
  void testFlushAndGet() {
    List<List<String>> flushedBatches = new ArrayList<>();

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(100, 100_000, flushedBatches::add, "test-thread", false)) {
      writer.start();

      writer.write("item1");
      writer.write("item2");

      List<String> batch = writer.flushAndGet();
      assertEquals(List.of("item1", "item2"), batch);
      assertEquals(0, writer.getBufferSize());
      assertEquals(0, flushedBatches.size());
    }
  }

  @Test
  void testScheduledFlush() throws InterruptedException {
    List<List<String>> flushedBatches = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(
            100,
            100,
            batch -> {
              flushedBatches.add(batch);
              latch.countDown();
            },
            "test-thread",
            false)) {
      writer.start();

      writer.write("item1");
      writer.write("item2");
      assertEquals(0, flushedBatches.size());

      assertTrue(latch.await(2, TimeUnit.SECONDS), "Scheduled flush should have triggered");
      assertEquals(1, flushedBatches.size());
      assertEquals(List.of("item1", "item2"), flushedBatches.get(0));
    }
  }

  @Test
  void testCloseFlushesRemainingBuffer() {
    List<List<String>> flushedBatches = new ArrayList<>();

    BufferedWriter<String> writer =
        new BufferedWriter<>(100, 100_000, flushedBatches::add, "test-thread", false);
    writer.start();

    writer.write("item1");
    writer.write("item2");
    assertEquals(0, flushedBatches.size());

    writer.close();
    assertEquals(1, flushedBatches.size());
    assertEquals(List.of("item1", "item2"), flushedBatches.get(0));
  }

  @Test
  void testWriteAfterCloseThrowsException() {
    BufferedWriter<String> writer =
        new BufferedWriter<>(100, 100_000, batch -> {}, "test-thread", false);
    writer.start();
    writer.close();

    assertThrows(IllegalStateException.class, () -> writer.write("item"));
  }

  @Test
  void testConcurrentWrites() throws InterruptedException {
    AtomicInteger totalFlushed = new AtomicInteger(0);

    try (BufferedWriter<Integer> writer =
        new BufferedWriter<>(
            10, 100_000, batch -> totalFlushed.addAndGet(batch.size()), "test-thread", false)) {
      writer.start();

      int numThreads = 5;
      int itemsPerThread = 100;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);

      for (int t = 0; t < numThreads; t++) {
        final int threadId = t;
        new Thread(
                () -> {
                  try {
                    startLatch.await();
                    for (int i = 0; i < itemsPerThread; i++) {
                      writer.write(threadId * 1000 + i);
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    doneLatch.countDown();
                  }
                })
            .start();
      }

      startLatch.countDown();
      doneLatch.await(10, TimeUnit.SECONDS);

      writer.flush();

      assertEquals(numThreads * itemsPerThread, totalFlushed.get());
    }
  }

  @Test
  void testEmptyFlushNoOp() {
    List<List<String>> flushedBatches = new ArrayList<>();

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(100, 100_000, flushedBatches::add, "test-thread", false)) {
      writer.start();

      writer.flush();
      assertEquals(0, flushedBatches.size());

      List<String> batch = writer.flushAndGet();
      assertTrue(batch.isEmpty());
    }
  }

  @Test
  void testMultipleStartCallsIgnored() {
    List<List<String>> flushedBatches = new ArrayList<>();

    try (BufferedWriter<String> writer =
        new BufferedWriter<>(100, 100, flushedBatches::add, "test-thread", false)) {
      writer.start();
      writer.start();
      writer.start();
    }
  }
}
