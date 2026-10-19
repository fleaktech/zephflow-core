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
package io.fleak.zephflow.lib.serdes.decode.internal;

import io.fleak.zephflow.lib.serdes.decode.ByteInput;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.RandomAccessInput;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import java.util.Objects;

/** A decoder for a format that needs random access; it is never decompressed as a whole. */
public final class RandomAccessDecoder implements RecordDecoder {

  private final String formatName;
  private final RandomAccessFormatFactory format;

  public RandomAccessDecoder(String formatName, RandomAccessFormatFactory format) {
    this.formatName = formatName;
    this.format = format;
  }

  /**
   * @throws IllegalStateException when the input offers no random access, which is the caller's
   *     mistake rather than bad data
   */
  @Override
  public RecordCursor open(ByteInput input, DecodeLimits limits) {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(limits, "limits");
    RandomAccessInput ranges =
        input
            .randomAccess()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        formatName + " needs an input with ByteInput.randomAccess()"));
    return new GuardedCursor(
        flags -> format.create(new GuardedRandomAccess(ranges, flags), limits, flags));
  }
}
