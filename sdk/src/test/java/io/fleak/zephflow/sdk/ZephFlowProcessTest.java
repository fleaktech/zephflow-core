package io.fleak.zephflow.sdk;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.runner.DagResult;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/20/25 */
public class ZephFlowProcessTest {

  @Test
  public void testProcess() {
    List<Datum> inputData = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      inputData.add(new Datum(i));
    }
    ZephFlow zephFlow = ZephFlow.startFlow();
    DagResult dagResult =
        zephFlow
            .filter("$.num % 2 == 0")
            .eval("dict_merge($, dict(type='even'))")
            .process(inputData, new NoSourceDagRunner.DagRunConfig(false, false));
    assertEquals(1, dagResult.getOutputEvents().size());
    assertEquals(
        fromJsonString(
"""
[
  {
    "num": 0,
    "type": "even"
  },
  {
    "num": 2,
    "type": "even"
  },
  {
    "num": 4,
    "type": "even"
  },
  {
    "num": 6,
    "type": "even"
  },
  {
    "num": 8,
    "type": "even"
  }
]
""",
            new TypeReference<List<RecordFleakData>>() {}),
        dagResult.getOutputEvents().values().stream().findFirst().orElseThrow());
  }

  private record Datum(int num) {}
}
