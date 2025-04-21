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
package io.fleak.zephflow.lib.commands.sink;

import io.fleak.zephflow.api.InitializedConfig;
import io.fleak.zephflow.api.metric.FleakCounter;
import java.io.IOException;

/**
 * Created by bolei on 9/3/24
 *
 * @param errorCounter error count during preparation phase
 * @param sinkErrorCounter error count during flushing phase
 */
public record SinkInitializedConfig<T>(
    SimpleSinkCommand.Flusher<T> flusher,
    SimpleSinkCommand.SinkMessagePreProcessor<T> messagePreProcessor,
    FleakCounter inputMessageCounter,
    FleakCounter errorCounter,
    FleakCounter sinkOutputCounter,
    FleakCounter outputSizeCounter,
    FleakCounter sinkErrorCounter)
    implements InitializedConfig {

  @Override
  public void close() throws IOException {
    if (flusher != null) {
      flusher.close();
    }
  }
}
