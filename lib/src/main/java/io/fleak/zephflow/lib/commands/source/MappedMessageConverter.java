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

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

/**
 * Decodes a source's own record type with a {@link MessageDecodingConverter}, through the event
 * {@code toEvent} makes of it. A failure is logged with {@code description} of the record.
 */
@RequiredArgsConstructor
public class MappedMessageConverter<T> implements RawDataConverter<T> {

  private final MessageDecodingConverter decoding;
  private final Function<T, SerializedEvent> toEvent;
  private final Function<T, String> description;

  @Override
  public ConvertedResult<T> convert(T sourceRecord, SourceExecutionContext<?> context) {
    ConvertedResult<SerializedEvent> result =
        decoding.convert(
            toEvent.apply(sourceRecord), context, () -> description.apply(sourceRecord));
    return result.error() == null
        ? ConvertedResult.success(result.transformedData(), sourceRecord)
        : ConvertedResult.failure(result.error(), sourceRecord);
  }
}
