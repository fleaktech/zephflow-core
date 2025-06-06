package io.fleak.zephflow.lib.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import lombok.SneakyThrows;

public class CompressionUtils {

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

  public static boolean isGzipped(byte[] data) {
    return data != null && data.length >= 2 && (data[0] & 0xFF) == 0x1F && (data[1] & 0xFF) == 0x8B;
  }
}
