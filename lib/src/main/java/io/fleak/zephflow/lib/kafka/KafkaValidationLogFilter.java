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

import java.util.Locale;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.filter.AbstractFilter;

public final class KafkaValidationLogFilter extends AbstractFilter {
  @Override
  public Result filter(LogEvent event) {
    if (event.getLoggerName() == null || !event.getLoggerName().startsWith("org.apache.kafka")) {
      return Result.NEUTRAL;
    }
    String template = event.getMessage().getFormattedMessage().toLowerCase(Locale.ROOT);
    if (event.getThrown() != null
        || template.contains("config")
        || template.contains("authenticat")
        || template.contains("sasl")
        || template.contains("password")
        || template.contains("credential")
        || template.contains("exception")) {
      return Result.DENY;
    }
    return Result.NEUTRAL;
  }
}
