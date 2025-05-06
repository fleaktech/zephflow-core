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
