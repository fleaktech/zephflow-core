package io.fleak.zephflow.lib.serdes.compression;

import io.fleak.zephflow.lib.serdes.SerializedEvent;

public class NoopDecompressor implements Decompressor {
  @Override
  public SerializedEvent decompress(SerializedEvent serializedEvent) {
    return serializedEvent;
  }
}
