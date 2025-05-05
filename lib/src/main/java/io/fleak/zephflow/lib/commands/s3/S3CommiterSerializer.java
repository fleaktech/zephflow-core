package io.fleak.zephflow.lib.commands.s3;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.List;

/** Created by bolei on 5/5/25 */
public interface S3CommiterSerializer<T> {

  byte[] serialize(List<T> events) throws Exception;

  class RecordFleakDataS3CommiterSerializer implements S3CommiterSerializer<RecordFleakData> {

    private final FleakSerializer<?> fleakSerializer;

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
