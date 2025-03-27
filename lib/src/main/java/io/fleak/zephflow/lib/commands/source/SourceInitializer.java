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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.util.Optional;

/** Created by bolei on 9/24/24 */
public class SourceInitializer<T> extends CommandInitializer {
  protected SourceInitializer(String nodeId, CommandPartsFactory commandPartsFactory) {
    super(nodeId, commandPartsFactory);
  }

  @Override
  public SourceInitializedConfig<T> initialize(
      String commandName, JobContext jobContext, CommandConfig commandConfig) {
    //noinspection unchecked
    SourceCommandPartsFactory<T> partsFactory = (SourceCommandPartsFactory<T>) commandPartsFactory;

    Fetcher<T> fetcher = partsFactory.createFetcher(commandConfig);
    RawDataEncoder<T> encoder = partsFactory.createRawDataEncoder(commandConfig);
    RawDataConverter<T> converter = partsFactory.createRawDataConverter(commandConfig);
    DlqWriter dlqWriter =
        Optional.ofNullable(jobContext)
            .map(JobContext::getDlqConfig)
            .map(partsFactory::createDlqWriter)
            .orElse(null);
    if (dlqWriter != null) {
      dlqWriter.open();
    }

    return new SourceInitializedConfig<>(fetcher, converter, encoder, dlqWriter);
  }
}
