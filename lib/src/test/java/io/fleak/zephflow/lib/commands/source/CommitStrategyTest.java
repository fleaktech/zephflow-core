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

import org.junit.jupiter.api.Test;

class CommitStrategyTest {

  @Test
  void testPerRecordCommitStrategy() {
    CommitStrategy strategy = PerRecordCommitStrategy.INSTANCE;
    
    assertEquals(CommitStrategy.CommitMode.PER_RECORD, strategy.getCommitMode());
    assertEquals(1, strategy.getCommitBatchSize());
    assertEquals(0, strategy.getCommitIntervalMs());
    
    assertTrue(strategy.shouldCommitNow(1, 0));
    assertTrue(strategy.shouldCommitNow(100, 1000));
  }

  @Test
  void testBatchCommitStrategyBySize() {
    CommitStrategy strategy = BatchCommitStrategy.ofBatchSize(100);
    
    assertEquals(CommitStrategy.CommitMode.BATCH, strategy.getCommitMode());
    assertEquals(100, strategy.getCommitBatchSize());
    assertEquals(0, strategy.getCommitIntervalMs());
    
    assertFalse(strategy.shouldCommitNow(99, 0));
    assertTrue(strategy.shouldCommitNow(100, 0));
    assertTrue(strategy.shouldCommitNow(150, 0));
  }

  @Test
  void testBatchCommitStrategyByTime() {
    CommitStrategy strategy = BatchCommitStrategy.ofInterval(5000);
    
    assertEquals(CommitStrategy.CommitMode.BATCH, strategy.getCommitMode());
    assertEquals(0, strategy.getCommitBatchSize());
    assertEquals(5000, strategy.getCommitIntervalMs());
    
    assertFalse(strategy.shouldCommitNow(10, 4999));
    assertTrue(strategy.shouldCommitNow(10, 5000));
    assertTrue(strategy.shouldCommitNow(10, 6000));
  }

  @Test
  void testBatchCommitStrategyBothSizeAndTime() {
    CommitStrategy strategy = new BatchCommitStrategy(100, 5000);
    
    assertEquals(CommitStrategy.CommitMode.BATCH, strategy.getCommitMode());
    assertEquals(100, strategy.getCommitBatchSize());
    assertEquals(5000, strategy.getCommitIntervalMs());
    
    // Should commit when either threshold is reached
    assertFalse(strategy.shouldCommitNow(99, 4999));
    assertTrue(strategy.shouldCommitNow(100, 4999)); // Size threshold
    assertTrue(strategy.shouldCommitNow(99, 5000));  // Time threshold
    assertTrue(strategy.shouldCommitNow(100, 5000)); // Both thresholds
  }

  @Test
  void testKafkaCommitStrategy() {
    CommitStrategy strategy = BatchCommitStrategy.forKafka();
    
    assertEquals(CommitStrategy.CommitMode.BATCH, strategy.getCommitMode());
    assertEquals(1000, strategy.getCommitBatchSize());
    assertEquals(5000, strategy.getCommitIntervalMs());
    
    assertFalse(strategy.shouldCommitNow(999, 4999));
    assertTrue(strategy.shouldCommitNow(1000, 0));
    assertTrue(strategy.shouldCommitNow(0, 5000));
  }

  @Test
  void testNoCommitStrategy() {
    CommitStrategy strategy = NoCommitStrategy.INSTANCE;
    
    assertEquals(CommitStrategy.CommitMode.NONE, strategy.getCommitMode());
    assertEquals(0, strategy.getCommitBatchSize());
    assertEquals(0, strategy.getCommitIntervalMs());
    
    assertFalse(strategy.shouldCommitNow(1, 0));
    assertFalse(strategy.shouldCommitNow(1000, 10000));
  }
}