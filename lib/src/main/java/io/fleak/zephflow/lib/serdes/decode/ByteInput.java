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
package io.fleak.zephflow.lib.serdes.decode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * The bytes to decode. Implemented by the caller; the decoder only calls it and does not own its
 * lifecycle. An implementation that knows the expected length must throw an {@link IOException} on
 * a premature end instead of returning -1, because formats without an end marker cannot detect
 * truncation themselves.
 */
@FunctionalInterface
public interface ByteInput {

  /** The raw (still compressed) bytes. Called at most once per cursor. */
  InputStream openStream() throws IOException;

  static ByteInput ofBytes(byte[] bytes) {
    return () -> new ByteArrayInputStream(bytes);
  }
}
