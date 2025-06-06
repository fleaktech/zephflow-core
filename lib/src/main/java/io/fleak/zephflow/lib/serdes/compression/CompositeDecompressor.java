package io.fleak.zephflow.lib.serdes.compression;

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CompositeDecompressor implements Decompressor {

  private final List<Decompressor> decompressors;

  @Override
  public SerializedEvent decompress(SerializedEvent serializedEvent) {
    var event = serializedEvent;
    for (var decompressor : decompressors) {
      event = decompressor.decompress(event);
    }
    return event;
  }
}
