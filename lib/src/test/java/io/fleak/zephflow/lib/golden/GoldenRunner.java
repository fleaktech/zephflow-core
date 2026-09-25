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
package io.fleak.zephflow.lib.golden;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.ClockAware;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.RandomAware;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Drives one fixture through a real command, single-threaded, with an injected clock and a seeded
 * random generator ({@link #GOLDEN_SEED}).
 *
 * <p>Limitation: events are fed one per {@code process} call and only the per-call output is
 * captured — output a command would emit from {@code terminate()} (an end-of-stream flush) is NOT
 * seen. Fine for the current per-event commands (parser, throttle); a windowed/aggregating command
 * that flushes on end-of-stream needs a flush-capture hook added here before it can be
 * golden-tested.
 */
final class GoldenRunner {

  static final long GOLDEN_SEED = 42L;

  record Result(List<ObjectNode> output, List<ObjectNode> errors) {}

  static Result run(GoldenCase c) throws IOException {
    Map<String, Object> config =
        JsonUtils.fromJsonString(Files.readString(c.config()), new TypeReference<>() {});

    var factory = OperatorCommandRegistry.OPERATOR_COMMANDS.get(c.command());
    if (factory == null) {
      throw new IllegalArgumentException("unknown command in fixture path: " + c.command());
    }
    OperatorCommand cmd = factory.createCommand("golden_" + c.name(), JOB_CONTEXT);
    if (!(cmd instanceof ScalarCommand scalar)) {
      throw new IllegalArgumentException(
          c.command() + " is not a ScalarCommand; sources/sinks are not golden-testable");
    }

    long[] now = {0L};
    boolean clockAware = cmd instanceof ClockAware;
    if (clockAware) {
      ((ClockAware) cmd).setClock(() -> now[0]);
    }

    if (cmd instanceof RandomAware randomAware) {
      randomAware.setRandom(new Random(GOLDEN_SEED));
    }

    scalar.parseAndValidateArg(config);
    scalar.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var ctx = scalar.getExecutionContext();

    List<ObjectNode> output = new ArrayList<>();
    List<ObjectNode> errors = new ArrayList<>();

    // One record at a time so a fixture can interleave time control with data.
    for (String line : Files.readAllLines(c.input())) {
      if (line.isBlank()) continue;
      ObjectNode node = (ObjectNode) JsonUtils.OBJECT_MAPPER.readTree(line);

      // _t / _advance are clock controls; they only mean anything for a ClockAware command.
      // For any other command they stay part of the record so real data named _t survives.
      if (clockAware) {
        if (node.has("_advance")) {
          if (node.size() != 1) {
            throw new IllegalArgumentException(
                "control record carrying _advance must have no other field: " + line);
          }
          now[0] += node.get("_advance").asLong();
          continue;
        }
        JsonNode t = node.remove("_t");
        if (t != null) now[0] = t.asLong();
      }

      RecordFleakData event = JsonUtils.fromJsonPayload(node);
      ScalarCommand.ProcessResult r = scalar.process(List.of(event), "golden", ctx);
      for (RecordFleakData out : r.getOutput()) output.add(JsonUtils.toJsonPayload(out));
      for (ErrorOutput err : r.getFailureEvents()) {
        ObjectNode e = JsonUtils.OBJECT_MAPPER.createObjectNode();
        e.set("input", JsonUtils.toJsonPayload(err.inputEvent()));
        e.put("error", err.errorMessage());
        errors.add(e);
      }
    }
    scalar.terminate();
    return new Result(output, errors);
  }
}
