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
package io.fleak.zephflow.lib.utils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Captures the actual logging boundary, including attached exceptions, for bounded privacy tests.
 */
public final class BoundedLogCapture implements AutoCloseable {
  private final Logger logger;
  private final Level originalLevel;
  private final List<LogEvent> events = new CopyOnWriteArrayList<>();
  private final AbstractAppender appender =
      new AbstractAppender(
          "bounded-log-capture",
          null,
          PatternLayout.createDefaultLayout(),
          false,
          Property.EMPTY_ARRAY) {
        @Override
        public void append(LogEvent event) {
          events.add(event.toImmutable());
        }
      };

  public BoundedLogCapture(Class<?> owner) {
    logger = (Logger) LogManager.getLogger(owner);
    originalLevel = logger.getLevel();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.DEBUG);
  }

  public List<LogEvent> events() {
    return List.copyOf(events);
  }

  @Override
  public void close() {
    logger.removeAppender(appender);
    logger.setLevel(originalLevel);
    appender.stop();
  }
}
