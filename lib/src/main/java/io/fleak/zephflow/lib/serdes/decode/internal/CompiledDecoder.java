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
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.decode.compress.Decompressors;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

public final class CompiledDecoder implements RecordDecoder {

  private final List<DecompressionCodec> codecs;
  private final FormatFactory format;

  public CompiledDecoder(List<DecompressionCodec> codecs, FormatFactory format) {
    this.codecs = codecs;
    this.format = format;
  }

  @Override
  public RecordCursor open(ByteInput input, DecodeLimits limits) {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(limits, "limits");
    return new GuardedCursor(flags -> start(input, limits, flags));
  }

  private FormatCursor start(ByteInput input, DecodeLimits limits, DecodeFlags flags)
      throws IOException {
    InputStream raw;
    try {
      raw = input.openStream();
    } catch (IOException | RuntimeException e) {
      flags.markRead(e);
      throw new SourceReadException("opening the input failed: " + e.getMessage(), e);
    }
    InputStream guarded = new ReadGuardStream(raw, flags);
    try {
      return format.create(Decompressors.wrap(codecs, guarded, flags), limits, flags);
    } catch (IOException | RuntimeException e) {
      guarded.close();
      throw e;
    }
  }
}
