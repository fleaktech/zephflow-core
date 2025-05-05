package io.fleak.zephflow.lib.commands.s3;

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.List;
import lombok.Getter;

/** Created by bolei on 5/5/25 */
public interface S3CommiterSerializer<T> {

  byte[] serialize(List<T> events) throws Exception;

  record RecordFleakDataS3CommiterSerializer(
      @VisibleForTesting @Getter FleakSerializer<?> fleakSerializer)
      implements S3CommiterSerializer<RecordFleakData> {

    public RecordFleakDataS3CommiterSerializer(FleakSerializer<?> fleakSerializer) {
      this.fleakSerializer = fleakSerializer;
    }

    @Override
    public byte[] serialize(List<RecordFleakData> events) throws Exception {
      SerializedEvent serializedEvent = fleakSerializer.serialize(events);
      return serializedEvent.value();
    }
  }
}
