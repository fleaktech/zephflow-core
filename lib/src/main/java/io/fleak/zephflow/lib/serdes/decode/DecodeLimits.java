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

/**
 * Memory bounds chosen by the caller. 0 means unlimited, which also means no memory protection.
 *
 * @param maxRecordBytes the largest single record: a line, a multiline event, a JSON array element
 * @param maxDocumentBytes the largest unit that has to be held whole, such as a JSON object
 *     document
 */
public record DecodeLimits(long maxRecordBytes, long maxDocumentBytes) {

  public static final DecodeLimits UNLIMITED = new DecodeLimits(0, 0);

  public DecodeLimits {
    if (maxRecordBytes < 0 || maxDocumentBytes < 0) {
      throw new IllegalArgumentException("limits must be >= 0 (0 means unlimited)");
    }
  }
}
