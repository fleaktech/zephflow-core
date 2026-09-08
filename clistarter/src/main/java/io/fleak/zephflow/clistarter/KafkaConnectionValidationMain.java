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
package io.fleak.zephflow.clistarter;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkDto;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import io.fleak.zephflow.lib.kafka.KafkaClientProperties;
import io.fleak.zephflow.lib.kafka.KafkaConnectionValidator;
import io.fleak.zephflow.lib.kafka.KafkaValidationLogFilter;
import io.fleak.zephflow.lib.utils.JsonUtils;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

public final class KafkaConnectionValidationMain {
  public record Check(String nodeId, KafkaConnectionValidator.Status status) {}

  public record Result(String status, List<Check> checks) {}

  public static void main(String[] args) {
    if (args.length != 3) return;
    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    var filter = new KafkaValidationLogFilter();
    var loggerConfigurations =
        new ArrayList<>(loggerContext.getConfiguration().getLoggers().values());
    loggerConfigurations.add(loggerContext.getConfiguration().getRootLogger());
    loggerConfigurations.forEach(configuration -> configuration.addFilter(filter));
    loggerContext.updateLoggers();
    run(args);
  }

  static void run(String[] args) {
    Result result;
    try {
      long remaining = Math.min(10000, Long.parseLong(args[2]) - System.currentTimeMillis());
      long deadline = System.nanoTime() + Math.max(0, remaining) * 1_000_000;
      result =
          validate(
              JsonUtils.OBJECT_MAPPER.readValue(
                  Files.readString(Path.of(args[0])), AdjacencyListDagDefinition.class),
              deadline);
    } catch (Exception failure) {
      result = new Result("UNAVAILABLE", List.of());
    }
    try {
      Files.writeString(Path.of(args[1]), JsonUtils.OBJECT_MAPPER.writeValueAsString(result));
    } catch (Exception ignored) {
    }
  }

  static Result validate(AdjacencyListDagDefinition definition, long deadline) {
    if (definition == null || definition.getDag() == null || definition.getDag().isEmpty())
      return new Result("UNAVAILABLE", List.of());
    var nodeIds = new HashSet<String>();
    for (var node : definition.getDag()) {
      if (node == null
          || node.getId() == null
          || node.getId().isBlank()
          || !nodeIds.add(node.getId())) return new Result("UNAVAILABLE", List.of());
    }
    JobContext context =
        definition.getJobContext() == null
            ? JobContext.builder().build()
            : definition.getJobContext();
    context.getOtherProperties().put(JobContext.FLAG_TEST_MODE, false);
    var checks = new ArrayList<Check>();
    var validator = new KafkaConnectionValidator();
    for (var node : definition.getDag()) {
      KafkaConnectionValidator.Status status;
      if (System.nanoTime() >= deadline) {
        status = KafkaConnectionValidator.Status.UNAVAILABLE;
      } else {
        try {
          Properties properties =
              switch (node.getCommandName()) {
                case "kafkasource" ->
                    KafkaClientProperties.source(
                        JsonUtils.OBJECT_MAPPER.convertValue(
                            node.getConfig(), KafkaSourceDto.Config.class));
                case "kafkasink" ->
                    KafkaClientProperties.sink(
                        JsonUtils.OBJECT_MAPPER.convertValue(
                            node.getConfig(), KafkaSinkDto.Config.class),
                        context);
                default -> throw new UnsupportedOperationException();
              };
          status =
              validator.validate(
                  properties, Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        } catch (UnsupportedOperationException failure) {
          status = KafkaConnectionValidator.Status.UNSUPPORTED;
        } catch (Exception failure) {
          status = KafkaConnectionValidator.Status.INVALID_CONFIGURATION;
        }
      }
      checks.add(new Check(node.getId(), status));
    }
    return new Result("SUCCESS", checks);
  }
}
