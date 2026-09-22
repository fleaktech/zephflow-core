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
package io.fleak.zephflow.lib.serdes.des;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import io.fleak.zephflow.lib.serdes.converters.TypedEventConverter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Created by bolei on 9/16/24 */
public class MultipleEventsDeserializer<T> extends FleakDeserializer<T> {
  private final MultipleEventsTypedDeserializer<T> multipleEventsTypedDeserializer;

  public MultipleEventsDeserializer(
      EncodingType encodingType,
      TypedEventConverter<T> typedEventConverter,
      MultipleEventsTypedDeserializer<T> multipleEventsTypedDeserializer) {
    super(encodingType, typedEventConverter);
    this.multipleEventsTypedDeserializer = multipleEventsTypedDeserializer;
  }

  @Override
  public List<RecordFleakData> deserialize(SerializedEvent serializedEvent) throws Exception {
    List<TypedEventContainer<T>> typedEvents =
        multipleEventsTypedDeserializer.deserializeMultiple(serializedEvent);
    return typedEvents.stream().map(typedEventConverter::typedEventToFleakData).toList();
  }

  @Override
  public void deserializeIncrementally(
      SerializedEvent event, Consumer<IncrementalRecord> consumer, BooleanSupplier stopRequested) {
    if (stopRequested.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      return;
    }
    if (event.value() == null) {
      consumer.accept(
          new IncrementalRecord(
              null, null, -1, -1, -1, new IllegalArgumentException("Transport payload is absent")));
      return;
    }
    Map<String, String> metadata = SerializedEvent.metadataWithKey(event);
    multipleEventsTypedDeserializer.deserializeIncrementally(
        event.value(),
        typed -> {
          if (stopRequested.getAsBoolean()) {
            return;
          }
          RecordFleakData record = null;
          Exception error = typed.error();
          if (error == null) {
            try {
              record =
                  typedEventConverter.typedEventToFleakData(
                      new TypedEventContainer<>(typed.value(), metadata));
            } catch (Exception e) {
              error = e;
            }
          }
          if (!stopRequested.getAsBoolean()) {
            consumer.accept(
                new IncrementalRecord(
                    record,
                    event.value(),
                    typed.index(),
                    typed.rawOffset(),
                    typed.rawLength(),
                    error));
          }
        },
        stopRequested);
  }

  @Override
  public DeserializationOutcome deserializeWithErrors(SerializedEvent serializedEvent) {
    if (!(multipleEventsTypedDeserializer
        instanceof LineOrientedTypedDeserializer<T> lineOrientedTypedDeserializer)) {
      return super.deserializeWithErrors(serializedEvent);
    }
    Map<String, String> metadata = SerializedEvent.metadataWithKey(serializedEvent);
    List<RecordFleakData> records = new ArrayList<>();
    List<DeserializationOutcome.RecordError> errors = new ArrayList<>();
    for (var lineOutcome :
        lineOrientedTypedDeserializer.deserializeEachLine(serializedEvent.value())) {
      if (lineOutcome.failed()) {
        errors.add(
            new DeserializationOutcome.RecordError(
                lineOutcome.line().text().getBytes(StandardCharsets.UTF_8),
                lineOutcome.line().number(),
                lineOutcome.error()));
        continue;
      }
      lineOutcome.events().stream()
          .map(event -> new TypedEventContainer<>(event, metadata))
          .map(typedEventConverter::typedEventToFleakData)
          .forEach(records::add);
    }
    return new DeserializationOutcome(records, errors);
  }

  @Override
  public boolean supportsChunkedPayloads() {
    return multipleEventsTypedDeserializer instanceof LineOrientedTypedDeserializer;
  }
}
