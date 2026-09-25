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
package io.fleak.zephflow.lib.serdes.compression;

import static io.fleak.zephflow.lib.utils.CompressionUtils.gunzip;
import static io.fleak.zephflow.lib.utils.CompressionUtils.isGzip;

import io.fleak.zephflow.lib.serdes.SerializedEvent;

/**
 * Gunzips a payload that starts with the gzip magic bytes and passes any other payload through
 * unchanged, so a source needs no compression setting and a topic may mix both. No JSON, CSV or
 * text payload starts with those bytes, so detection cannot misread an uncompressed one.
 */
public class GzipDetectingDecompressor implements Decompressor {
  @Override
  public SerializedEvent decompress(SerializedEvent serializedEvent) {
    return isGzip(serializedEvent.value())
        ? serializedEvent.copyWithValue(gunzip(serializedEvent.value()))
        : serializedEvent;
  }
}
