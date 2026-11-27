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
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RemovedCommandCompilationTest {
  @Test
  void syslogUdpSourceIsAnUnknownCommand() {
    var definition =
        AdjacencyListDagDefinition.builder()
            .jobContext(JobContext.builder().build())
            .dag(
                List.of(
                    AdjacencyListDagDefinition.DagNode.builder()
                        .id("source")
                        .commandName("syslogudp")
                        .config(Map.of())
                        .outputs(List.of("sink"))
                        .build(),
                    AdjacencyListDagDefinition.DagNode.builder()
                        .id("sink")
                        .commandName("stdout")
                        .config(
                            Map.of(
                                "encoding",
                                Map.of("format", Map.of("type", "json", "mode", "lines"))))
                        .build()))
            .build();

    var failure =
        assertThrows(
            DagCompilationException.class,
            () ->
                new DagCompiler(OperatorCommandRegistry.OPERATOR_COMMANDS)
                    .compile(definition, true));

    assertEquals(
        List.of(
            DagCompilationException.ErrorType.UNKNOWN_COMMAND,
            "source",
            "syslogudp",
            "unknown command: syslogudp"),
        List.of(
            failure.getErrorType(),
            failure.getNodeId(),
            failure.getCommandName(),
            failure.getMessage()));
  }
}
