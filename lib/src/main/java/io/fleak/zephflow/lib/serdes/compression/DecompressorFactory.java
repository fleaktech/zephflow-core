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
