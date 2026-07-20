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
package io.fleak.zephflow.lib.commands.sqssink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SqsSinkCommandTest {

  private static class TestableSqsSinkCommand extends SqsSinkCommand {
    TestableSqsSinkCommand() {
      super(null, null, null, null, null);
    }

    void setConfig(SqsSinkDto.Config config) {
      this.commandConfig = config;
    }

    int callBatchSize() {
      return batchSize();
    }
  }

  private SqsSinkDto.Config config(Integer batchSize) {
    return SqsSinkDto.Config.builder()
        .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
        .regionStr("us-east-1")
        .encodingType("JSON_OBJECT")
        .batchSize(batchSize)
        .build();
  }

  @Test
  void batchSize_honorsConfiguredValue() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(3));
    assertEquals(3, cmd.callBatchSize());
  }

  @Test
  void batchSize_clampsAboveMaxToMax() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(50));
    assertEquals(SqsSinkDto.MAX_BATCH_SIZE, cmd.callBatchSize());
  }

  @Test
  void batchSize_clampsNonPositiveToOne() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(0));
    assertEquals(1, cmd.callBatchSize());
  }

  @Test
  void batchSize_defaultsWhenNull() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(null));
    assertEquals(SqsSinkDto.DEFAULT_BATCH_SIZE, cmd.callBatchSize());
  }
}
