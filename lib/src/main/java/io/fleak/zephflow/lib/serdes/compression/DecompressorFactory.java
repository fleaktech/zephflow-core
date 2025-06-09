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

import io.fleak.zephflow.lib.serdes.CompressionType;
import java.util.List;

public class DecompressorFactory {
  private static final NoopDecompressor DEFAULT = new NoopDecompressor();

  public static Decompressor getDecompressor(List<CompressionType> compressionTypes) {
    if (compressionTypes == null || compressionTypes.isEmpty()) {
      return DEFAULT;
    }
    return new CompositeDecompressor(
        compressionTypes.stream().map(DecompressorFactory::getDecompressor).toList());
  }

  public static Decompressor getDecompressor(CompressionType compressionType) {
    if (compressionType == null) {
      return DEFAULT;
    }

    // we need to add more decompressors, keep it as a switch
    return switch (compressionType) {
      case GZIP -> new GzipDecompressor();
    };
  }
}
