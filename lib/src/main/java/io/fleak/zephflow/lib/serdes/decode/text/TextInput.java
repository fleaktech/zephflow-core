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
package io.fleak.zephflow.lib.serdes.decode.text;

import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer;
import io.fleak.zephflow.lib.serdes.decode.internal.Transcoding;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

final class TextInput {

  private TextInput() {}

  static LineFramer lineFramer(InputStream in, Charset charset, long maxRecordBytes)
      throws IOException {
    InputStream utf8 = Transcoding.toUtf8(in, charset);
    return new LineFramer(utf8, maxRecordBytes, utf8 == in);
  }

  static void close(LineFramer framer, InputStream in) {
    if (framer != null) {
      framer.close();
      return;
    }
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing was read.
    }
  }
}
