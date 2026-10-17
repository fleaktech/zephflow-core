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

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads of a bounded range at any offset, for formats that start from a footer (parquet).
 * Implemented by the caller like {@link ByteInput}; every call may do I/O. A cursor asks for the
 * length once and then reads ranges one at a time, in increasing order within a row group; the
 * bytes must not change while it reads.
 */
public interface RandomAccessInput {

  long length() throws IOException;

  /**
   * The bytes in {@code [offset, offset + length)}, which lie within {@link #length()}. The decoder
   * reads at most {@code length} bytes and closes the stream; ending early is a read failure.
   */
  InputStream read(long offset, long length) throws IOException;
}
