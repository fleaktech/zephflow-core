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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;

/**
 * Serializes an {@link S3EventMessage} as the original SQS message body (the S3 event
 * notification), keyed by message id — the same shape the fetcher writes to the DLQ, so the DLQ and
 * raw-data sampling share one schema.
 */
public class S3RealtimeRawDataEncoder implements RawDataEncoder<S3EventMessage> {
  @Override
  public SerializedEvent serialize(S3EventMessage sourceRecord) {
    byte[] key =
        sourceRecord.messageId() == null
            ? null
            : sourceRecord.messageId().getBytes(StandardCharsets.UTF_8);
    byte[] value =
        sourceRecord.rawBody() == null
            ? new byte[0]
            : sourceRecord.rawBody().getBytes(StandardCharsets.UTF_8);
    return new SerializedEvent(key, value, null);
  }
}
