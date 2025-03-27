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
package io.fleak.zephflow.lib.commands.s3;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.time.Instant;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Slf4j
public class S3Flusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  final S3Client s3Client;
  final String bucketName;
  final String keyName;
  final FleakSerializer<?> fleakSerializer;

  public S3Flusher(
      S3Client s3Client, String bucketName, String keyName, FleakSerializer<?> fleakSerializer) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.fleakSerializer = fleakSerializer;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents) throws Exception {
    List<RecordFleakData> events = preparedInputEvents.preparedList();

    SerializedEvent serializedEvent = fleakSerializer.serialize(events);
    String fileKey =
        String.format(
            "%s/%s.%s",
            keyName,
            Instant.now().toEpochMilli(),
            fleakSerializer.getEncodingType().getFileExtension());
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(bucketName).key(fileKey).build();
    s3Client.putObject(putObjectRequest, RequestBody.fromBytes(serializedEvent.value()));

    return new SimpleSinkCommand.FlushResult(events.size(), List.of());
  }

  @Override
  public void close() {
    s3Client.close();
  }
}
