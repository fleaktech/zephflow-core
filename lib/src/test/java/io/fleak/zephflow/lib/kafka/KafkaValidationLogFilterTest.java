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
package io.fleak.zephflow.lib.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

class KafkaValidationLogFilterTest {
  @Test
  void realKafkaLoggerRetainsNetworkAndLifecycleWhileDroppingRawAuthentication() {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    var configuration = context.getConfiguration().getRootLogger();
    var filter = new KafkaValidationLogFilter();
    List<String> messages = new ArrayList<>();
    var appender =
        new AbstractAppender("validation-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
          }
        };
    appender.start();
    configuration.addAppender(appender, Level.ALL, null);
    var loggerConfigurations = new ArrayList<>(context.getConfiguration().getLoggers().values());
    loggerConfigurations.add(configuration);
    loggerConfigurations.forEach(loggerConfiguration -> loggerConfiguration.addFilter(filter));
    context.updateLoggers();
    try {
      var logger = LogManager.getLogger("org.apache.kafka.clients.NetworkClient");
      logger.warn("Connection to node {} could not be established. Node may not be available.", -1);
      logger.warn("Node {} disconnected.", -1);
      logger.warn(
          "Connection to node {} failed authentication due to: {}", -1, "SENTINEL_PASSWORD");
      logger.warn("Sending {} request: {}", "SASL_AUTHENTICATE", "SENTINEL_PASSWORD");
      logger.error("Unexpected error", new IllegalArgumentException("SENTINEL_PASSWORD"));
      LogManager.getLogger("org.apache.kafka.clients.admin.AdminClientConfig")
          .warn("AdminClientConfig values: {}", "SENTINEL_PASSWORD");
      assertEquals(2, messages.size());
      assertTrue(
          messages.stream().anyMatch(message -> message.contains("could not be established")));
      assertFalse(messages.toString().contains("SENTINEL_PASSWORD"));
    } finally {
      loggerConfigurations.forEach(loggerConfiguration -> loggerConfiguration.removeFilter(filter));
      configuration.removeAppender(appender.getName());
      context.updateLoggers();
      appender.stop();
    }
  }
}
