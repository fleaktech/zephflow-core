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
package io.fleak.zephflow.lib.commands.sample;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_CALLING_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.KeyedStatefulCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.WindowFlushable;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class SampleCommandTest {

  private static final String USER = "u1";

  private static final class ScriptedRandom implements RandomGenerator {
    private final Deque<Integer> script;
    private final Integer constant;
    final List<Integer> bounds = new ArrayList<>();

    private ScriptedRandom(Deque<Integer> script, Integer constant) {
      this.script = script;
      this.constant = constant;
    }

    static ScriptedRandom always(int value) {
      return new ScriptedRandom(new ArrayDeque<>(), value);
    }

    static ScriptedRandom sequence(Integer... values) {
      return new ScriptedRandom(new ArrayDeque<>(List.of(values)), null);
    }

    @Override
    public int nextInt(int bound) {
      bounds.add(bound);
      return constant != null ? constant : script.removeFirst();
    }

    @Override
    public long nextLong() {
      throw new UnsupportedOperationException();
    }
  }

  private record Increment(String metric, Map<String, String> tags) {}

  private static final class RecordingMetricClientProvider
      extends MetricClientProvider.NoopMetricClientProvider {
    final List<Increment> increments = new ArrayList<>();

    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          increments.add(new Increment(name, additionalTags));
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          for (long i = 0; i < n; i++) {
            increase(additionalTags);
          }
        }
      };
    }
  }

  private static SampleCommand command(
      String configJson, RandomGenerator rng, MetricClientProvider metrics) {
    SampleCommand cmd = (SampleCommand) new SampleCommandFactory().createCommand("n1", JOB_CONTEXT);
    cmd.setRandom(rng);
    cmd.parseAndValidateArg(JsonUtils.fromJsonString(configJson, new TypeReference<>() {}));
    cmd.initialize(metrics);
    return cmd;
  }

  private static SampleCommand command(String configJson, RandomGenerator rng) {
    return command(configJson, rng, new MetricClientProvider.NoopMetricClientProvider());
  }

  private static RecordFleakData rec(String json) {
    return (RecordFleakData)
        FleakData.wrap(JsonUtils.fromJsonString(json, new TypeReference<Map<String, Object>>() {}));
  }

  private static List<RecordFleakData> run(SampleCommand cmd, List<RecordFleakData> events) {
    ScalarCommand.ProcessResult r = cmd.process(events, USER, cmd.getExecutionContext());
    assertEquals(List.of(), r.getFailureEvents());
    return r.getOutput();
  }

  private static List<RecordFleakData> runOneByOne(
      SampleCommand cmd, List<RecordFleakData> events) {
    List<RecordFleakData> out = new ArrayList<>();
    events.forEach(e -> out.addAll(run(cmd, List.of(e))));
    return out;
  }

  private static List<RecordFleakData> ids(int from, int to) {
    List<RecordFleakData> events = new ArrayList<>();
    for (int i = from; i <= to; i++) {
      events.add(rec("{\"id\": " + i + "}"));
    }
    return events;
  }

  @Test
  void injectedGeneratorAlwaysZeroKeepsLastOfEachBatch() {
    ScriptedRandom rng = ScriptedRandom.always(0);
    SampleCommand cmd = command("{\"rules\": [{\"sampleRate\": 3}]}", rng);

    assertEquals(
        List.of(rec("{\"id\": 3, \"__sampled__\": 3}"), rec("{\"id\": 6, \"__sampled__\": 3}")),
        runOneByOne(cmd, ids(1, 7)));
    assertEquals(List.of(2, 3, 2, 3), rng.bounds);
  }

  @Test
  void injectedGeneratorAlwaysOneKeepsFirstOfEachBatch() {
    ScriptedRandom rng = ScriptedRandom.always(1);
    SampleCommand cmd = command("{\"rules\": [{\"sampleRate\": 3}]}", rng);

    assertEquals(
        List.of(rec("{\"id\": 1, \"__sampled__\": 3}"), rec("{\"id\": 4, \"__sampled__\": 3}")),
        runOneByOne(cmd, ids(1, 7)));
    assertEquals(List.of(2, 3, 2, 3), rng.bounds);
  }

  @Test
  void keptEventIsEmittedAtItsBatchesLastPositionInOneProcessCall() {
    SampleCommand cmd =
        command(
            """
            {"rules": [
              {"condition": "$.r == 1", "sampleRate": 2},
              {"condition": "$.r == 2", "sampleRate": 3}
            ]}""",
            ScriptedRandom.always(1));
    RecordFleakData a1 = rec("{\"id\": \"a1\", \"r\": 1}");
    RecordFleakData a2 = rec("{\"id\": \"a2\", \"r\": 1}");
    RecordFleakData b1 = rec("{\"id\": \"b1\", \"r\": 2}");
    RecordFleakData b2 = rec("{\"id\": \"b2\", \"r\": 2}");
    RecordFleakData b3 = rec("{\"id\": \"b3\", \"r\": 2}");
    RecordFleakData p1 = rec("{\"id\": \"p1\", \"r\": 3}");
    RecordFleakData p2 = rec("{\"id\": \"p2\"}");
    RecordFleakData p3 = rec("{\"id\": \"p3\", \"r\": \"1\"}");

    assertEquals(
        List.of(
            p1,
            rec("{\"id\": \"a1\", \"r\": 1, \"__sampled__\": 2}"),
            p2,
            rec("{\"id\": \"b1\", \"r\": 2, \"__sampled__\": 3}"),
            p3),
        run(cmd, List.of(a1, p1, b1, a2, p2, b2, b3, p3)));
  }

  @Test
  void metricsAreCountedWithTheTagsOfTheEventTheyCount() {
    RecordingMetricClientProvider metrics = new RecordingMetricClientProvider();
    SampleCommand cmd =
        command(
            """
            {"rules": [
              {"condition": "($.a / $.b) > 1", "sampleRate": 2},
              {"condition": "$.m == true", "sampleRate": 3}
            ]}""",
            ScriptedRandom.sequence(0, 1),
            metrics);
    RecordFleakData p1 = rec("{\"__tag__\": {\"id\": \"p1\"}, \"m\": false}");
    RecordFleakData e1 = rec("{\"__tag__\": {\"id\": \"e1\"}, \"m\": true, \"a\": 4, \"b\": 0}");
    RecordFleakData e2 = rec("{\"__tag__\": {\"id\": \"e2\"}, \"m\": true}");
    RecordFleakData e3 = rec("{\"__tag__\": {\"id\": \"e3\"}, \"m\": true}");
    RecordFleakData e4 = rec("{\"__tag__\": {\"id\": \"e4\"}, \"m\": true}");

    List<RecordFleakData> output = new ArrayList<>();
    output.addAll(run(cmd, List.of(p1, e1)));
    output.addAll(run(cmd, List.of(e2, e3, e4)));

    assertEquals(
        List.of(p1, rec("{\"__tag__\": {\"id\": \"e2\"}, \"m\": true, \"__sampled__\": 3}")),
        output);
    assertEquals(
        List.of(
            new Increment(METRIC_NAME_INPUT_EVENT_COUNT, tags("p1")),
            new Increment(METRIC_NAME_OUTPUT_EVENT_COUNT, tags("p1")),
            new Increment(METRIC_NAME_INPUT_EVENT_COUNT, tags("e1")),
            new Increment(METRIC_NAME_INPUT_EVENT_COUNT, tags("e2")),
            new Increment("sample_dropped_count", tags("e1")),
            new Increment(METRIC_NAME_INPUT_EVENT_COUNT, tags("e3")),
            new Increment("sample_dropped_count", tags("e3")),
            new Increment(METRIC_NAME_OUTPUT_EVENT_COUNT, tags("e2")),
            new Increment(METRIC_NAME_INPUT_EVENT_COUNT, tags("e4"))),
        metrics.increments);
  }

  private static Map<String, String> tags(String id) {
    return Map.of(METRIC_TAG_CALLING_USER, USER, "id", id);
  }

  @Test
  void inputEventsAreNeverMutated() {
    SampleCommand cmd =
        command(
            "{\"rules\": [{\"condition\": \"$.k == true\", \"sampleRate\": 2}]}",
            ScriptedRandom.always(1));
    RecordFleakData kept =
        rec("{\"id\": 1, \"k\": true, \"__sampled__\": 9, \"req\": {\"path\": \"/a\"}}");
    RecordFleakData pass = rec("{\"id\": 2, \"k\": false, \"__sampled__\": 4}");
    RecordFleakData dropped = rec("{\"id\": 3, \"k\": true}");
    List<RecordFleakData> inputs = List.of(kept, pass, dropped);
    List<RecordFleakData> copies = inputs.stream().map(RecordFleakData::deepCopy).toList();

    List<RecordFleakData> output = new ArrayList<>(run(cmd, List.of(kept, pass)));
    output.addAll(run(cmd, List.of(dropped)));

    assertEquals(
        List.of(
            pass, rec("{\"id\": 1, \"k\": true, \"__sampled__\": 2, \"req\": {\"path\": \"/a\"}}")),
        output);
    assertEquals(copies, inputs);
  }

  @Test
  void sampleRateFieldIsALiteralTopLevelKey() {
    SampleCommand cmd =
        command(
            "{\"rules\": [{\"sampleRate\": 1}], \"sampleRateField\": \"a.b\"}",
            ScriptedRandom.always(0));

    assertEquals(
        List.of(rec("{\"a\": {\"b\": 5}, \"x\": 1, \"a.b\": 1}")),
        run(cmd, List.of(rec("{\"a\": {\"b\": 5}, \"x\": 1}"))));
  }

  @Test
  void endOfInputEmitsEachIncompleteGroupWithItsActualSizeThenResets() {
    SampleCommand cmd =
        command(
            "{\"rules\": [{\"condition\": \"$.id < 10\", \"sampleRate\": 10},"
                + " {\"condition\": \"$.id >= 10\", \"sampleRate\": 4}]}",
            ScriptedRandom.always(1));

    assertEquals(
        List.of(),
        runOneByOne(
            cmd,
            List.of(
                rec("{\"id\": 1}"), rec("{\"id\": 2}"), rec("{\"id\": 3}"), rec("{\"id\": 11}"))));
    assertEquals(
        List.of(rec("{\"id\": 1, \"__sampled__\": 3}"), rec("{\"id\": 11, \"__sampled__\": 1}")),
        cmd.flushAtEndOfInput(USER, cmd.getExecutionContext()));
    assertEquals(List.of(), cmd.flushAtEndOfInput(USER, cmd.getExecutionContext()));

    assertEquals(List.of(), run(cmd, List.of(rec("{\"id\": 4}"))));
    assertEquals(
        List.of(rec("{\"id\": 4, \"__sampled__\": 1}")),
        cmd.flushAtEndOfInput(USER, cmd.getExecutionContext()));
  }

  @Test
  void endOfInputOutputIsCountedWithTheCandidatesTags() {
    RecordingMetricClientProvider metrics = new RecordingMetricClientProvider();
    SampleCommand cmd =
        command("{\"rules\": [{\"sampleRate\": 5}]}", ScriptedRandom.always(1), metrics);
    run(
        cmd,
        List.of(rec("{\"__tag__\": {\"id\": \"e1\"}}"), rec("{\"__tag__\": {\"id\": \"e2\"}}")));
    metrics.increments.clear();

    cmd.flushAtEndOfInput(USER, cmd.getExecutionContext());

    assertEquals(
        List.of(new Increment(METRIC_NAME_OUTPUT_EVENT_COUNT, tags("e1"))), metrics.increments);
  }

  @Test
  void isKeyedStatefulButNotWindowFlushable() {
    SampleCommand cmd = (SampleCommand) new SampleCommandFactory().createCommand("n1", JOB_CONTEXT);
    assertInstanceOf(KeyedStatefulCommand.class, cmd);
    assertFalse(cmd instanceof WindowFlushable);
  }
}
