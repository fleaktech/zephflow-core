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
package io.fleak.zephflow.lib.commands.source;

import io.fleak.zephflow.api.InitializedConfig;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.IOException;

/** Created by bolei on 9/23/24 */
public record SourceInitializedConfig<T>(
    Fetcher<T> fetcher,
    RawDataConverter<T> converter,
    RawDataEncoder<T> encoder,
    FleakCounter dataSizeCounter,
    FleakCounter inputEventCounter,
    FleakCounter deserializeFailureCounter,
    DlqWriter dlqWriter)
    implements InitializedConfig {
  @Override
  public void close() throws IOException {
    if (fetcher != null) {
      fetcher.close();
    }
    if (dlqWriter != null) {
      dlqWriter.close();
    }
  }
}
