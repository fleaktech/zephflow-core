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
package io.fleak.zephflow.lib.serdes.decode.compress;

import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.List;

public final class Decompressors {

  private Decompressors() {}

  /** Applies the codecs in order: the first entry undoes the outermost compression. */
  public static InputStream wrap(List<DecompressionCodec> codecs, InputStream in, DecodeFlags flags)
      throws IOException {
    InputStream current = in;
    for (DecompressionCodec codec : codecs) {
      current =
          switch (codec) {
            case GZIP -> new GzipMemberStream(current, flags);
            case AUTO -> autoDetect(current, flags);
          };
    }
    return current;
  }

  private static InputStream autoDetect(InputStream in, DecodeFlags flags) throws IOException {
    PushbackInputStream pushback = new PushbackInputStream(in, 2);
    byte[] magic = new byte[2];
    int read = 0;
    while (read < 2) {
      int n = pushback.read(magic, read, 2 - read);
      if (n < 0) {
        break;
      }
      read += n;
    }
    if (read > 0) {
      pushback.unread(magic, 0, read);
    }
    boolean gzip = read == 2 && (magic[0] & 0xFF) == 0x1f && (magic[1] & 0xFF) == 0x8b;
    return gzip ? new GzipMemberStream(pushback, flags) : pushback;
  }
}
