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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer.TypedOutcome;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Shared mechanics for bounded decoding. No helper collects the complete output sequence. */
public final class IncrementalSupport {
  private IncrementalSupport() {}

  public static final class Stopped extends RuntimeException {
    private Stopped() {
      super(null, null, false, false);
    }
  }

  public static final class SinkFailure extends RuntimeException {
    public final RuntimeException failure;

    private SinkFailure(RuntimeException failure) {
      this.failure = failure;
    }
  }

  public static void checkStop(BooleanSupplier stop) {
    if (stop.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      throw new Stopped();
    }
  }

  /** Keep downstream/storage failures distinct from malformed source input. */
  public static <T> void deliver(Consumer<T> consumer, T outcome) {
    try {
      consumer.accept(outcome);
    } catch (RuntimeException e) {
      throw new SinkFailure(e);
    }
  }

  public record RawLine(int offset, int length) {
    public String text(byte[] value) {
      return new String(value, offset, length, StandardCharsets.UTF_8);
    }
  }

  /**
   * CR, LF and CRLF follow the existing String.lines/BufferedReader rules, including final lines.
   */
  public static void forEachLine(byte[] value, Consumer<RawLine> consumer, BooleanSupplier stop) {
    int start = 0;
    for (int i = 0; i < value.length; i++) {
      if ((i & 1023) == 0) {
        checkStop(stop);
      }
      if (value[i] == '\n' || value[i] == '\r') {
        checkStop(stop);
        consumer.accept(new RawLine(start, i - start));
        if (value[i] == '\r' && i + 1 < value.length && value[i + 1] == '\n') {
          i++;
        }
        start = i + 1;
      }
    }
    if (start < value.length) {
      checkStop(stop);
      consumer.accept(new RawLine(start, value.length - start));
    }
  }

  private static JsonParser parser(
      byte[] value, int offset, int length, boolean line, BooleanSupplier stop) throws IOException {
    // JSON lines historically decode UTF-8 into String (replacement semantics); preserve that while
    // keeping raw bytes separately. JSON_ARRAY historically parses the byte input directly.
    JsonParser parser =
        line
            ? OBJECT_MAPPER
                .getFactory()
                .createParser(new String(value, offset, length, StandardCharsets.UTF_8))
            : OBJECT_MAPPER.getFactory().createParser(value, offset, length);
    return new JsonParserDelegate(parser) {
      @Override
      public JsonToken nextToken() throws IOException {
        checkStop(stop);
        return super.nextToken();
      }

      @Override
      public String nextFieldName() throws IOException {
        return nextToken() == JsonToken.FIELD_NAME ? currentName() : null;
      }

      @Override
      public JsonToken nextValue() throws IOException {
        JsonToken token = nextToken();
        return token == JsonToken.FIELD_NAME ? nextToken() : token;
      }
    };
  }

  private static void validateObject(JsonParser parser) throws IOException {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("Expected a JSON object");
    }
    int depth = 1;
    while (depth > 0) {
      JsonToken token = parser.nextToken();
      if (token == null) {
        throw new IllegalArgumentException("Incomplete JSON object");
      }
      if (token.isStructStart()) {
        depth++;
      } else if (token.isStructEnd()) {
        depth--;
      } else if (token.isNumeric()) {
        parser.getNumberValue();
      } else if (token == JsonToken.VALUE_STRING) {
        parser.getText();
      }
    }
  }

  /**
   * Two passes preserve atomic document/line errors without retaining an array of parsed objects.
   */
  public static void visitJson(
      byte[] value,
      int offset,
      int length,
      boolean line,
      boolean allowObject,
      Consumer<TypedOutcome<ObjectNode>> consumer,
      BooleanSupplier stop) {
    try {
      boolean object;
      try (JsonParser parser = parser(value, offset, length, line, stop)) {
        object = validateJsonDocument(parser, allowObject);
      }
      checkStop(stop);
      try (JsonParser parser = parser(value, offset, length, line, stop)) {
        parser.nextToken();
        int index = 0;
        if (object) {
          ObjectNode node =
              OBJECT_MAPPER
                  .readerFor(ObjectNode.class)
                  .without(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                  .readValue(parser);
          checkStop(stop);
          deliver(consumer, new TypedOutcome<>(node, ++index, offset, length, null));
        } else {
          while (parser.nextToken() != JsonToken.END_ARRAY) {
            ObjectNode node =
                OBJECT_MAPPER
                    .readerFor(ObjectNode.class)
                    .without(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .readValue(parser);
            checkStop(stop);
            deliver(consumer, new TypedOutcome<>(node, ++index, -1, -1, null));
          }
        }
      }
    } catch (Stopped ignored) {
      // Cancellation is not malformed input.
    } catch (SinkFailure e) {
      throw e.failure;
    } catch (Exception e) {
      if (!stop.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
        consumer.accept(new TypedOutcome<>(null, -1, offset, length, e));
      }
    }
  }

  private static boolean validateJsonDocument(JsonParser parser, boolean allowObject)
      throws IOException {
    JsonToken first = parser.nextToken();
    boolean object = allowObject && first == JsonToken.START_OBJECT;
    if (object) {
      validateObject(parser);
    } else {
      if (first != JsonToken.START_ARRAY) {
        throw new IllegalArgumentException("Expected a JSON array");
      }
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        validateObject(parser);
      }
    }
    if (parser.nextToken() != null) {
      throw new IllegalArgumentException("Trailing JSON content");
    }
    return object;
  }
}
