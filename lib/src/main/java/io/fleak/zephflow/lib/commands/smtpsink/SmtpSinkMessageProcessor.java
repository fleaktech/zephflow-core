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
package io.fleak.zephflow.lib.commands.smtpsink;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.TemplatePlaceholderResolver;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SmtpSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<PreparedEmail> {

  private final SmtpSinkDto.Config config;
  private final TemplatePlaceholderResolver toResolver;
  private final TemplatePlaceholderResolver ccResolver;
  private final TemplatePlaceholderResolver subjectResolver;
  private final TemplatePlaceholderResolver bodyResolver;

  public SmtpSinkMessageProcessor(SmtpSinkDto.Config config) {
    this.config = config;
    this.toResolver = TemplatePlaceholderResolver.create(config.getToTemplate());
    this.ccResolver =
        config.getCcTemplate() != null
            ? TemplatePlaceholderResolver.create(config.getCcTemplate())
            : null;
    this.subjectResolver = TemplatePlaceholderResolver.create(config.getSubjectTemplate());
    this.bodyResolver = TemplatePlaceholderResolver.create(config.getBodyTemplate());
  }

  @Override
  public PreparedEmail preprocess(RecordFleakData event, long ts) {
    String toResolved = toResolver.resolvePlaceholders(event);
    List<String> toList = splitAddresses(toResolved);

    List<String> ccList = List.of();
    if (ccResolver != null) {
      String ccResolved = ccResolver.resolvePlaceholders(event);
      ccList = splitAddresses(ccResolved);
    }

    String subject = subjectResolver.resolvePlaceholders(event);
    String body = bodyResolver.resolvePlaceholders(event);

    return new PreparedEmail(
        config.getFromAddress(), toList, ccList, subject, body, config.getBodyContentType());
  }

  private static List<String> splitAddresses(String addresses) {
    return Arrays.stream(addresses.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
