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
            fleakSerializer.getEncodingType().getFileExtension());
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
