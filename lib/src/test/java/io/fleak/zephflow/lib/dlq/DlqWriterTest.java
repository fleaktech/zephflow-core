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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.lib.deadletter.DeadLetter;
import java.util.List;
import org.junit.jupiter.api.Test;

class DlqWriterTest {

  @Test
  void writeNowIsUnsupportedUnlessOverridden() {
    DlqWriter writer = new BufferOnlyDlqWriter();

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> writer.writeNow(List.of()));

    assertEquals("BufferOnlyDlqWriter cannot write now", exception.getMessage());
  }

  private static class BufferOnlyDlqWriter extends DlqWriter {
    @Override
    protected void doWrite(DeadLetter deadLetter) {}

    @Override
    public void open() {}

    @Override
    public void close() {}
  }
}
