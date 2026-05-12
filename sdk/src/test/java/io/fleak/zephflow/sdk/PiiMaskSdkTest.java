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
package io.fleak.zephflow.sdk;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Config;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.DetectorConfig;
import io.fleak.zephflow.lib.commands.piimask.PiiMaskCommandDto.Detectors;
import io.fleak.zephflow.runner.DagResult;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PiiMaskSdkTest {

  @Test
  void piiMaskInPipelineRedactsEmail() {
    Config cfg =
        new Config(
            List.of("$.message"),
            new Detectors(new DetectorConfig(null), null, null, null, null, null),
            null);

    DagResult result =
        ZephFlow.startFlow()
            .piiMask(cfg)
            .process(
                List.of(new Event("ping a@b.com please")),
                new NoSourceDagRunner.DagRunConfig(false, false));

    Map<String, List<RecordFleakData>> outputs = result.getOutputEvents();
    assertEquals(1, outputs.size(), "expected exactly one exit node");
    List<RecordFleakData> events = outputs.values().iterator().next();
    assertEquals(1, events.size());
    assertEquals("ping [EMAIL] please", ((Map<?, ?>) events.get(0).unwrap()).get("message"));
  }

  private record Event(String message) {}
}
