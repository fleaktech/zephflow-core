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
package io.fleak.zephflow.lib.dlq;

import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Optional;

/** Created by bolei on 11/5/24 */
public abstract class DlqWriter implements Closeable {
  public void writeToDlq(long processingTs, SerializedEvent serializedEvent, String errorMsg) {
    DeadLetter.Builder builder =
        DeadLetter.newBuilder()
            .setProcessingTimestamp(processingTs)
            .setErrorMessage(errorMsg)
            .setMetadata(serializedEvent.metadata());

    Optional.ofNullable(serializedEvent.key()).ifPresent(k -> builder.setKey(ByteBuffer.wrap(k)));
    Optional.ofNullable(serializedEvent.value())
        .ifPresent(v -> builder.setValue(ByteBuffer.wrap(v)));
    DeadLetter deadLetter = builder.build();
    doWrite(deadLetter);
  }

  protected abstract void doWrite(DeadLetter deadLetter);

  public abstract void open();
}
