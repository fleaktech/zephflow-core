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
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class PiiMaskExecutionContext extends DefaultExecutionContext {

  List<DottedPath> targets;
  List<PiiMasker.Spec> specs;
  FleakCounter piiMatchCounter;

  public PiiMaskExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      List<DottedPath> targets,
      List<PiiMasker.Spec> specs,
      FleakCounter piiMatchCounter) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.targets = targets;
    this.specs = specs;
    this.piiMatchCounter = piiMatchCounter;
  }

  @Override
  public void close() {
    // No resources to release
  }
}
