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
package io.fleak.zephflow.lib.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import lombok.SneakyThrows;

public class CompressionUtils {

  private static final int GZIP_MAGIC_FIRST_BYTE = 0x1f;
  private static final int GZIP_MAGIC_SECOND_BYTE = 0x8b;

  /** Whether {@code data} starts with the gzip magic bytes; false for null or short input. */
  public static boolean isGzip(byte[] data) {
    return data != null
        && data.length >= 2
        && (data[0] & 0xff) == GZIP_MAGIC_FIRST_BYTE
        && (data[1] & 0xff) == GZIP_MAGIC_SECOND_BYTE;
  }

  @SneakyThrows
  public static byte[] gunzip(byte[] data) {
    try (var bis = new ByteArrayInputStream(data);
        var gis = new GZIPInputStream(bis);
        var bos = new ByteArrayOutputStream()) {

      byte[] buffer = new byte[4096];
      int len;
      while ((len = gis.read(buffer)) != -1) {
        bos.write(buffer, 0, len);
      }
      return bos.toByteArray();
    }
  }
}
