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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

class BoundedDagCompilerTest {
  @Test
  void compilationFailureDoesNotExposeResolvedConfigurationOrExceptionMessage() {
    String credential = "private-resolved-credential";
    CommandFactory factory = mock(CommandFactory.class);
    OperatorCommand command = mock(OperatorCommand.class);
    when(factory.commandType()).thenReturn(CommandType.INTERMEDIATE_COMMAND);
    when(factory.createCommand(anyString(), any())).thenReturn(command);
    doThrow(new IllegalArgumentException("invalid endpoint token=" + credential))
        .when(command)
        .parseAndValidateArg(any());
    JobContext context =
        JobContext.builder()
            .otherProperties(new HashMap<>(Map.of(JobContext.FLAG_BOUNDED_MODE, true)))
            .build();
    var definition =
        AdjacencyListDagDefinition.builder()
            .jobContext(context)
            .dag(
                List.of(
                    AdjacencyListDagDefinition.DagNode.builder()
                        .id("external")
                        .commandName("external")
                        .config(Map.of("token", credential))
                        .build()))
            .build();
    List<LogEvent> logs = new ArrayList<>();
    Logger logger = (Logger) LogManager.getLogger(DagCompiler.class);
    AbstractAppender appender =
        new AbstractAppender(
            "bounded-compiler-test",
            null,
            PatternLayout.createDefaultLayout(),
            false,
            Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            logs.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      DagCompilationException failure =
          assertThrows(
              DagCompilationException.class,
              () -> new DagCompiler(Map.of("external", factory)).compile(definition, true));
      assertEquals(DagCompilationException.ErrorType.NODE_COMPILATION, failure.getErrorType());
      assertEquals("external", failure.getNodeId());
      assertEquals("external", failure.getCommandName());
      assertTrue(failure.getMessage().contains("IllegalArgumentException"));
      assertFalse(failure.getMessage().contains(credential));
      assertNull(failure.getCause());
      assertEquals(1, logs.size());
      assertFalse(logs.getFirst().getMessage().getFormattedMessage().contains(credential));
      assertNull(logs.getFirst().getThrown());
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }
}
