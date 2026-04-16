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
package io.fleak.zephflow.lib.commands.imapsource;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record EmailMessage(
    String messageId,
    String from,
    List<String> to,
    List<String> cc,
    String subject,
    String bodyText,
    String bodyHtml,
    Map<String, String> headers,
    List<Attachment> attachments,
    Instant receivedDate) {

  public record Attachment(String filename, String contentType, String contentBase64) {}
}
