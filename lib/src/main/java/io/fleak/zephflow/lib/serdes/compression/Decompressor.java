package io.fleak.zephflow.lib.serdes.compression;

import io.fleak.zephflow.lib.serdes.SerializedEvent;

public interface Decompressor {
  SerializedEvent decompress(SerializedEvent serializedEvent);
}
