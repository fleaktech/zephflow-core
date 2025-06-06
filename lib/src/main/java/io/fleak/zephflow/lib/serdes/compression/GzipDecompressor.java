package io.fleak.zephflow.lib.serdes.compression;

import static io.fleak.zephflow.lib.utils.CompressionUtils.gunzip;

import io.fleak.zephflow.lib.serdes.SerializedEvent;

public class GzipDecompressor implements Decompressor {

  @Override
  public SerializedEvent decompress(SerializedEvent serializedEvent) {
    return serializedEvent.updateValue(gunzip(serializedEvent.value()));
  }
}
