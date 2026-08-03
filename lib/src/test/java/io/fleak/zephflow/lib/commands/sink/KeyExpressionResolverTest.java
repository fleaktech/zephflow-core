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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyExpressionResolverTest {

  private CapturingAppender appender;
  private Logger resolverLogger;

  @BeforeEach
  void attachAppender() {
    appender = new CapturingAppender();
    appender.start();
    resolverLogger = (Logger) LogManager.getLogger(KeyExpressionResolver.class);
    resolverLogger.addAppender(appender);
  }

  @AfterEach
  void detachAppender() {
    resolverLogger.removeAppender(appender);
    appender.stop();
  }

  @Test
  void resolvesNumericValueWithoutDecimalPoint() {
    KeyExpressionResolver resolver =
        new KeyExpressionResolver(
            PathExpression.fromString("$.id"), "partitionKeyFieldExpressionStr", "no key is set");

    assertEquals("4", resolver.resolve(FleakData.wrap(Map.of("id", 4))));
    assertTrue(appender.messages.isEmpty(), "a resolved key must not warn");
  }

  @Test
  void resolvesBooleanValue() {
    KeyExpressionResolver resolver =
        new KeyExpressionResolver(
            PathExpression.fromString("$.active"), "partitionKeyFieldExpressionStr", "no key");

    assertEquals("true", resolver.resolve(FleakData.wrap(Map.of("active", true))));
  }

  @Test
  void passesAnEmptyStringThroughAsAKey() {
    KeyExpressionResolver resolver =
        new KeyExpressionResolver(
            PathExpression.fromString("$.id"), "partitionKeyFieldExpressionStr", "no key");

    // Deliberately unchanged: an empty string is a value, not a missing field, so it stays a key.
    // (Kafka and Event Hub accept it; Kinesis rejects it, which is pre-existing behavior for any
    // string-typed key expression and is not this resolver's call to silently paper over.)
    assertEquals("", resolver.resolve(FleakData.wrap(Map.of("id", ""))));
    assertTrue(appender.messages.isEmpty());
  }

  @Test
  void returnsNullForNonScalarValue() {
    KeyExpressionResolver resolver =
        new KeyExpressionResolver(
            PathExpression.fromString("$.id"), "partitionKeyFieldExpressionStr", "no key");

    assertNull(resolver.resolve(FleakData.wrap(Map.of("id", Map.of("nested", 1)))));
  }

  @Test
  void warnsOnceWhenTheKeyCannotBeResolved() {
    KeyExpressionResolver resolver =
        new KeyExpressionResolver(
            PathExpression.fromString("$.id"),
            "partitionKeyFieldExpressionStr",
            "records are sent without a partition key");

    FleakData event = FleakData.wrap(Map.of("id", Map.of("nested", 1)));
    resolver.resolve(event);
    resolver.resolve(event);
    resolver.resolve(event);

    assertEquals(1, appender.messages.size(), "the warning must be logged once per resolver");
    String warning = appender.messages.getFirst();
    assertTrue(warning.contains("partitionKeyFieldExpressionStr"), warning);
    assertTrue(warning.contains("$.id"), warning);
    assertTrue(warning.contains("records are sent without a partition key"), warning);
    // The offending record is included so the warning is actionable without a repro.
    assertTrue(warning.contains("\"nested\""), warning);
  }

  @Test
  void unconfiguredResolverReturnsNullWithoutWarning() {
    KeyExpressionResolver resolver =
        new KeyExpressionResolver(null, "partitionKeyFieldExpressionStr", "no key");

    assertNull(resolver.resolve(FleakData.wrap(Map.of("id", 4))));
    assertTrue(appender.messages.isEmpty(), "an unconfigured key must not warn");
  }

  private static class CapturingAppender extends AbstractAppender {
    private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

    CapturingAppender() {
      super("KeyExpressionResolverTestAppender", null, null, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }
  }
}
