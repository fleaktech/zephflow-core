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

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.time.Instant;
import java.util.List;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** Created by bolei on 4/25/25 */
public class OnDemandS3Commiter extends S3Commiter<RecordFleakData> {

  @VisibleForTesting final String keyName;
  @VisibleForTesting final FleakSerializer<?> fleakSerializer;

  public OnDemandS3Commiter(
      S3Client s3Client, String bucketName, String keyName, FleakSerializer<?> fleakSerializer) {
    super(s3Client, bucketName);
    this.keyName = keyName;
    this.fleakSerializer = fleakSerializer;
  }

  @Override
  public long commit(List<RecordFleakData> events) throws Exception {
    SerializedEvent serializedEvent = fleakSerializer.serialize(events);
    String fileKey =
        String.format(
            "%s/%s.%s",
            keyName,
            Instant.now().toEpochMilli(),
            fleakSerializer.getEncodingTypes().getFirst().getFileExtension());
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(bucketName).key(fileKey).build();
    s3Client.putObject(putObjectRequest, RequestBody.fromBytes(serializedEvent.value()));
    return serializedEvent.value().length;
  }

  @Override
  public void close() {
    s3Client.close();
  }
}
