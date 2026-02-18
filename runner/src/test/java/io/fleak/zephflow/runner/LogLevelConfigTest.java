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
package io.fleak.zephflow.runner;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LogLevelConfigTest {

  private static final String ZEPHFLOW_LOGGER = "io.fleak.zephflow";

  @AfterEach
  void resetLogLevel() {
    Configurator.setLevel(ZEPHFLOW_LOGGER, Level.INFO);
  }

  @Test
  void testApplyLogLevel_setsDebugLevel() {
    JobContext ctx = JobContext.builder().logLevel("debug").build();
    DagExecutor.applyLogLevel(ctx);
    assertEquals(Level.DEBUG, LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel());
  }

  @Test
  void testApplyLogLevel_caseInsensitive() {
    JobContext ctx = JobContext.builder().logLevel("DEBUG").build();
    DagExecutor.applyLogLevel(ctx);
    assertEquals(Level.DEBUG, LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel());
  }

  @Test
  void testApplyLogLevel_nullLogLevel_noChange() {
    Level before = LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel();
    JobContext ctx = JobContext.builder().build();
    DagExecutor.applyLogLevel(ctx);
    assertEquals(before, LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel());
  }

  @Test
  void testApplyLogLevel_nullJobContext_noException() {
    Level before = LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel();
    assertDoesNotThrow(() -> DagExecutor.applyLogLevel(null));
    assertEquals(before, LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel());
  }

  @Test
  void testApplyLogLevel_invalidLevel_noChange() {
    Level before = LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel();
    JobContext ctx = JobContext.builder().logLevel("notavalidlevel").build();
    DagExecutor.applyLogLevel(ctx);
    assertEquals(before, LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel());
  }

  @Test
  void testApplyLogLevel_blankString_noChange() {
    Level before = LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel();
    JobContext ctx = JobContext.builder().logLevel("  ").build();
    DagExecutor.applyLogLevel(ctx);
    assertEquals(before, LogManager.getLogger(ZEPHFLOW_LOGGER).getLevel());
  }
}
